package br.ufmg.cs.systems.sparktuner

import br.ufmg.cs.systems.sparktuner.model._

import org.json4s._
import org.json4s.native.JsonMethods._

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.commons.math3.stat.descriptive.moment.Skewness

import scala.io.Source
import java.net.URI

import org.apache.spark.Logging

class Analyzer(logFile: URI,
    corrThreshold: Double = 0.7,
    skewnessThreshold: Double = 1) extends Logging {
  import Analyzer._

  logInfo (s"Analyzer initialized from ${logFile}")
  
  def this(logPath: String) = this(new URI(logPath))
    
  // parse log file (fill model)
  private val stages: Seq[(RDD,List[Stage])] = groupByRDD (parseLogFile).
    toSeq.sortBy (_._1.id)

  /** policies for optimization **/
  private var _policies: Map[String,(RDD,List[Stage]) => Action] = Map(
    "empty-tasks" -> optForEmptyTasks,
    "spill" -> optForSpill,
    "garbage-collection" -> optForGc,
    "task-imbalance" -> optForTaskImbalance
    )

  /**
   * Users can add their custom policies through this call. The default policies
   * are:
   * - Empty Tasks:
   * - Spill:
   * - Garbage Collection:
   * - Task Imbalance: 
   *
   * @param func new policy receives the RDD representing an adaptive point and
   * a list of respective stages starting in this AP and returns an [[Action]]
   */
  def addPolicy(name: String, func: (RDD,List[Stage]) => Action): Unit = {
    _policies += (name -> func)
  }

  def policies = _policies

  private def skewness(values: Array[Long]): Double =
    skewness (values.map(_.toDouble))

  def skewness(values: Array[Double]): Double =
    skewnessCalc.evaluate (values)

  def highSkewness(skewness: Double): Boolean = {
    skewness.abs > skewnessThreshold
  }

  def correlation(values1: Array[Long], values2: Array[Long]): Double = {
    if (values1.sum == 0 || values2.sum == 0)
      0.0
    else {
      pCorrelation.correlation (values1.map(_.toDouble),
        values2.map(_.toDouble)) match {
          case corr if corr.isNaN => 1.0
          case corr => corr
      }
    }
  }

  def highCorrelation(corr: Double): Boolean = {
    corr.abs >= corrThreshold
  }
  
  private def getRepr(stages: List[Stage]): Stage =
    stages.maxBy (_.rdds.size)

  def getActions: Map[String,Seq[Action]] = {
    val iteratives = isIterative
    val regulars = isRegular

    // separate by categories
    var apPoints = Map.empty[String,Seq[(RDD,List[Stage])]].
      withDefaultValue (Seq.empty[(RDD,List[Stage])])

    for ((rdd,stages) <- stages) {
      apPoints += (rdd.name -> ((rdd,stages) +: apPoints(rdd.name)))
    }

    // iterate over all adaptive points and instances
    var allActions = Map.empty[String,Seq[Action]]
    for ((ap,instances) <- apPoints) {
      val iterative = iteratives(ap)
      val regular = regulars(ap)
      val sorted_instances = instances.sortBy (_._1.id)
      // decide and optimize based on the application type
      (iterative, regular) match {
        case (true, true) => // iterative and regular
          val actions = optIterativeRegular (sorted_instances)
          allActions = allActions ++ actions

        case (true, false) => // iterative and irregular
          val actions = optIterativeIrregular (sorted_instances)
          allActions = allActions ++ actions

        case (false, _) => // noniterative
          val actions = optNoniterative (sorted_instances)
          allActions = allActions ++ actions

        case _ =>
          throw new RuntimeException (s"Could not determine the AP(${ap}) category")
      }
    }

    allActions
  }

  private def optNoniterative(instances: Seq[(RDD,List[Stage])])
      : Map[String,Seq[Action]] = {
    var actions = Map.empty[String,Seq[Action]].withDefaultValue (Seq.empty[Action])
    
    for ((rdd,stages) <- instances) {
      val action = optAP (rdd, stages)
      actions += (rdd.name -> (actions(rdd.name) :+ action))
    }

    actions
  }

  private def optIterativeRegular(instances: Seq[(RDD,List[Stage])])
      : Map[String,Seq[Action]] = {
    var actions = Map.empty[String,Seq[Action]].
      withDefaultValue (Seq.empty[Action])
    
    for ((rdd,stages) <- instances) actions.get (rdd.name) match {
      case Some(apActions) =>
        actions += (rdd.name -> (apActions :+ apActions.head))

      case None =>
        val _actions = optNoniterative (Seq((rdd,stages)))
        actions += (rdd.name -> _actions.values.head)
    }

    actions
  }

  private def optIterativeIrregular(instances: Seq[(RDD,List[Stage])])
      : Map[String,Seq[Action]] = {
    var actions = Map.empty[String,Seq[Action]]
    var firstInput = Map.empty[String,Long]

    for ((rdd,stages) <- instances)
        (actions.get(rdd.name), firstInput.get(rdd.name)) match {
      case (Some(apActions), Some(fi)) => // scale optimization
        val repr = getRepr (stages)
        val factor = repr.recordsRead / fi.toDouble
        val firstAction = apActions.head
        val action = firstAction.scaled (factor)
        actions += (rdd.name -> (apActions :+ action))

      case _ => // first iteration
        val repr = getRepr (stages)
        val _actions = optNoniterative (Seq((rdd,stages)))
        actions += (rdd.name -> _actions.values.head)
        firstInput += (rdd.name -> repr.recordsRead)
    }

    actions
  }

  private def optAP(rdd: RDD, stages: List[Stage]): Action = {
    for ((name,optFunc) <- policies.iterator) {
      val action = optFunc (rdd, stages)
      if (action.valid) {
        return action.setPolicySrc (name)
      }
    }
    NOAction (rdd.name)
  }


  /** Default policies */
  private def optForEmptyTasks(rdd: RDD, stages: List[Stage]): Action = {
    logDebug (s"${rdd}: optimizing for emptyTasks")
    val repr = getRepr (stages)
    val emptyTasks = repr.emptyTasks
    val numEmptyTasks = emptyTasks.size
    if (numEmptyTasks > 0) {
      UNPAction (rdd.name, (repr.numTasks - numEmptyTasks).toInt)
    } else {
      NOAction (rdd.name)
    }
  }

  private def optForSpill(rdd: RDD, stages: List[Stage]): Action = {
    logDebug (s"${rdd}: optimizing for spill")
    val repr = getRepr (stages)
    val shuffleWriteBytes = repr.shuffleWriteBytes
    if (shuffleWriteBytes > 0) {
      val factor = repr.bytesSpilled / shuffleWriteBytes.toDouble
      if (factor > 0) {
        val newNumPartitions = math.ceil (factor * repr.numTasks).toInt
        UNPAction (rdd.name, newNumPartitions)
      } else {
        NOAction (rdd.name)
      }
    } else {
      NOAction (rdd.name)
    }
  }

  private def optForGc(rdd: RDD, stages: List[Stage]): Action = {
    logDebug (s"${rdd}: optimizing for GC")
    val repr = getRepr (stages)
    val gcTimes = repr.taskGcTimes
    val gcOverheads = repr.taskGcOverheads
    val _skewness = skewness (gcOverheads)
    percentileCalc.setData (gcOverheads)

    val q1 = percentileCalc.evaluate (25)
    val q3 = percentileCalc.evaluate (75)
    logDebug (s"q1 = ${q1}; q3 = ${q3}")

    val iq = q3 - q1
    val uif = q3 + 1.5 * iq
    val sk = skewnessCalc.evaluate (gcOverheads)
    logDebug (s"skewness measure = ${sk}; upper inner fence = ${uif}")

    if (highSkewness (_skewness)) {

      // how many outliers there are weighted proportionally to uif
      val inc = gcOverheads.
        filter(_ > uif).
        map (go => (go / (uif max 1)).toInt max 1).
        sum

      val newNumPartitions = (repr.numTasks + inc).toInt
      UNPAction (rdd.name, newNumPartitions)

    } else {
      NOAction (rdd.name)
    }
  }

  sealed trait SourceOfImbalance
  case object NoImbalance extends SourceOfImbalance
  case object KeyDist extends SourceOfImbalance
  case object Inherent extends SourceOfImbalance
  case object VariableCost extends SourceOfImbalance

  private def sourceOfImbalance(stage: Stage): SourceOfImbalance = {
    val runTimes = stage.taskRunTimes
    val _skewness = skewness (runTimes)
    if (!highSkewness(_skewness)) return NoImbalance

    val corr1 = correlation (runTimes, stage.taskShuffleReadBytes)
    val corr2 = correlation (runTimes, stage.taskShuffleReadRecords)
    (highCorrelation (corr1), highCorrelation (corr2)) match {
      case (true, true) => KeyDist
      case (false, false) => Inherent
      case _ => VariableCost
    }
  }

  private def optForTaskImbalance(rdd: RDD, stages: List[Stage]): Action = {
    logDebug (s"${rdd}: optimizing for taskImbalance")
    val repr = getRepr (stages)
    sourceOfImbalance (repr) match {
      case Inherent =>
        NOAction (rdd.name)

      case KeyDist =>
        UPAction (rdd.name, "rangePartitioner")

      case VariableCost =>
        NOAction (rdd.name)
        
      case NoImbalance =>
        NOAction (rdd.name)
    }
  }
  /*****/

  private def isIterative: Map[String,Boolean] = {
    var iterative = Map.empty[String,Int].withDefaultValue (0)
    for ((rdd,stages) <- stages.iterator)
      iterative += (rdd.name -> (iterative(rdd.name) + 1))
    iterative.mapValues (_ > 1)
  }

  private def isRegular: Map[String,Boolean] = {
    var regular = Map.empty[String,Map[String,Long]]

    for ((rdd,stages) <- stages.iterator) {
      val recordsRead = stages.map (s => (s.name,s.recordsRead)).toMap
      regular.get (rdd.name) match {
        case Some(_recordsRead) if equivInputs (recordsRead, _recordsRead) =>
          regular += (rdd.name -> _recordsRead)
        case Some(_recordsRead) if equivInputs (_recordsRead, recordsRead) =>
          regular += (rdd.name -> recordsRead)
        case Some(_) =>
          regular += (rdd.name -> Map.empty[String,Long])
        case None =>
          regular += (rdd.name -> recordsRead)
      }
    }

    regular.mapValues (!_.isEmpty)
  }

  private def groupByRDD(stages: Map[Long,Stage]): Map[RDD,List[Stage]] = {
    var grouped = Map.empty[RDD,List[Stage]].withDefaultValue (Nil)

    for ((stageId,stage) <- stages.iterator) if (stage.canAdapt) {
      val firstRDD = stage.firstRDD
      grouped += (firstRDD -> (stage :: grouped(firstRDD)))
    }

    grouped
  }

  private def parseLogFile: Map[Long,Stage] = {
    var stages = Map.empty[Long,Stage]

    for (line <- Source.fromFile(logFile).getLines) {
      val strInput: JsonInput = new StringInput(line)
      val jsonData = parse (strInput, false, false)

      jsonData \ "Event" match {

        case JString("SparkListenerTaskEnd") =>
          val task = new Task(jsonData)
          stages(task.stageId).addTask (task)

        case JString("SparkListenerStageSubmitted") =>
          val stage = new Stage(jsonData)
          assert (!(stages contains stage.id))
          stages += (stage.id -> stage)

        case _ =>
      }
    }

    stages
  }

}

object Analyzer {

  lazy val pCorrelation = new PearsonsCorrelation
  lazy val percentileCalc = new Percentile
  lazy val skewnessCalc = new Skewness

  def main (args: Array[String]) {
    val logPath = args(0)
    val actions = new Analyzer (logPath).getActions
    println (s"\n\n:: Actions :: \n\n${actions.mkString("\n")}")
  }

  /**
   * Get values in json4s form
   */
  @scala.annotation.tailrec
  def getJValue(jsonData: JValue, keys: String*): JValue = {
    val _jsonData = jsonData \ keys(0)
    val _keys = keys.drop (1)
    if (!_keys.isEmpty)
      getJValue (_jsonData, _keys:_*)
    else
      _jsonData
  }

  /**
   * Get value as type 'T'
   */
  def getValue [T] (jsonData: JValue, keys: String*): T = {
    getJValue (jsonData, keys:_*) match {
      case JNothing =>
        null.asInstanceOf[T]
      case JInt(bi : BigInt) =>
        bi.toLong.asInstanceOf[T]
      case jvalue =>
        jvalue.values.asInstanceOf[T]
    }
  }

  /**
   * Returns true if 'superset' contains all elements from 'subset'
   */
  def equivInputs [K,V] (subset: Map[K,V], superset: Map[K,V]): Boolean = {
    val (subsetSeq, supersetSeq) = (subset.toSeq, superset.toSeq)
    for (it <- subsetSeq) if (!(supersetSeq.contains(it))) return false
    true
  }

}
