package br.ufmg.cs.systems.sparktuner

import br.ufmg.cs.systems.util.PrivateMethodExposer.p

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.serializer.Serializer
import org.apache.spark.Partitioner.defaultPartitioner

import scala.reflect.ClassTag

trait Action extends Logging {
  // we consider an RDD as possible adaptive points
  def ap: String

  // some action may be augmented or diminished by a factor
  def scaled(factor: Double): Action = this

  // only 'NoAction' by default is considered a non valid action
  def valid = true

  def actionApplied [T] (res: T): T = {
    logInfo (s"${this}:${policySrc} was applied and produced: ${res}")
    res
  }

  private var _policySrc: String = ""

  /**
   * Policy name that originated this action
   */
  def policySrc: String = _policySrc

  def setPolicySrc(os: String): Action = {
    _policySrc = os
    this
  }
}

/**
 * Applied when the number of partitions of an adaptive point must change based
 * on some criteria.
 */
case class UNPAction(ap: String, numPartitions: Int) extends Action {
  override def scaled(factor: Double): Action = {
    val newNumPartitions = math.ceil (factor * numPartitions).toInt max 1
    UNPAction (ap, newNumPartitions).setPolicySrc (policySrc)
  }
}

/**
 * Applied when the partitioning strategy of some adaptive point must be
 * changed, like hashPartitioner -> rangePartitioner
 */
case class UPAction(ap: String, partitionerStr: String) extends Action


/**
 * Applied when there is nothing else to do regarding this adaptive point
 * TODO: include optional message in NOAction (warn)
 */
case class NOAction(ap: String) extends Action {
  override def valid = false
}

/**
 * This class helps developers to include optmizations action in their code.
 */
class OptHelper(apToActions: Map[String,Seq[Action]]) extends Logging {
  var currPos = Map.empty[String,Int].withDefaultValue (0)

  def this() = {
    this (Map.empty[String,Seq[Action]])
  }

  private def nextPos(point: String) = {
    val pos = currPos (point)
    if (pos < apToActions(point).size) {
      currPos += (point -> (pos+1))
      pos
    } else {
      logWarning (s"Execution plan is too small(${point},${pos},${currPos.size}), reusing last action.")
      apToActions.size - 1
    }
  }

  /**
   * Get a partitioner based on the current adaptative point.
   *
   * @param rdd the unadapted RDD
   * @param point the name that makes reference to an adaptive point
   *
   * @return spark partitioner adapted (or not) by the respective action
   */
  private def getPartitionerWithNext[K: ClassTag, V: ClassTag](
      rdd: RDD[(K,V)],
      point: String)
    (implicit orderingOpt: Option[Ordering[K]] = None): Partitioner =
    apToActions(point)(nextPos(point)) match {

    case act @ NOAction(ap) =>
      act.actionApplied(rdd.partitioner.get)

    case act @ UNPAction(ap, numPartitions) =>
      act.actionApplied (new HashPartitioner(numPartitions))

    case act @ UPAction(ap, "rangePartitioner") if orderingOpt.isDefined =>
      implicit val ordering = orderingOpt.get
      act.actionApplied (
        new RangePartitioner(rdd.partitioner.get.numPartitions,
          rdd.dependencies.head.rdd.asInstanceOf[RDD[_ <: Product2[K,V]]])
      )

    case act @ UPAction(ap, "hashPartitioner") =>
      act.actionApplied (new HashPartitioner(rdd.partitioner.get.numPartitions))

    case action =>
      throw new RuntimeException (s"Unrecognized Action: ${action}")
  }

  /**
   * Get a partitioner based on the current adaptative point.
   *
   * @param prev the RDD that will originate an RDD representing an adaptive
   * point
   * @param point the name that makes reference to an adaptive point
   *
   * @return spark partitioner adapted (or not) by the respective action
   */
  def getPartitioner[K: Ordering: ClassTag, V: ClassTag](
      point: String,
      defaultNumPartitions: Int = -1,
      prev: RDD[(K,V)] = null): Partitioner = apToActions.get(point) match {
    
    case Some(actions) => actions(nextPos(point)) match {
      case act @ NOAction(ap) if defaultNumPartitions == -1 =>
        act.actionApplied (defaultPartitioner (prev))
      
      case act @ NOAction(ap) =>
        act.actionApplied (new HashPartitioner(defaultNumPartitions))

      case act @ UNPAction(ap, numPartitions) =>
        act.actionApplied (new HashPartitioner(numPartitions))

      case act @ UPAction(ap, "rangePartitioner") =>
        act.actionApplied (new RangePartitioner(defaultPartitioner(prev).numPartitions,
          prev))

      case act @ UPAction(ap, "hashPartitioner") =>
        act.actionApplied (new HashPartitioner(defaultPartitioner(prev).numPartitions))

      case action =>
        throw new RuntimeException (s"Unrecognized Action: ${action}")
    }

    case None if defaultNumPartitions == -1 =>
      defaultPartitioner(prev)

    case None =>
      new HashPartitioner(defaultNumPartitions)
  }

  def getNumPartitions(
      defaultNumPartitions: Int,
      point: String): Int = apToActions.get(point) match {

    case Some(actions) => actions(nextPos(point)) match {
      case act @ NOAction(ap) => act.actionApplied(defaultNumPartitions)

      case act @ UNPAction(ap, numPartitions) => act.actionApplied (numPartitions)

      case act @ UPAction(_, _) => act.actionApplied (defaultNumPartitions)

      case action =>
        throw new RuntimeException (s"Unrecognized Action: ${action}")
    }
    
    case None => defaultNumPartitions

  }

  /**
   * Users call this function to get an potentially adapted RDD if actions are
   * associated to it
   *
   * @param point the adaptive point name
   * @param rdd RDD that must be adapted
   * @param prev RDD that originated `rdd` through an operator (e.g. reduceByKey
   * and join)
   *
   * @return adapted RDD or the same RDD if that could not be done
   */
  def adaptRDD[K: ClassTag, V: ClassTag, C: ClassTag](
      point: String,
      rdd: RDD[(K,C)],
      prev: RDD[(K,V)]): RDD[(K,C)] = rdd match {
    case shRdd: ShuffledRDD[K,V,C] =>
      adaptShuffledRDD (point, shRdd, shRdd.prev).
        setName (point)
    case cgRdd: CoGroupedRDD[K] => 
      adaptCoGroupedRDD (point, cgRdd)
    case _ =>
      rdd.setName (point)
  }

  // TODO: create adapt functions for other common RDDs that could represent a
  // adaptive point

  /** adapt CoGroupedRDD */
  private def adaptCoGroupedRDD[K: ClassTag](point: String, rdd: CoGroupedRDD[K]) = apToActions.get (point) match {
    case Some(actions) =>
      val cgRdd = new CoGroupedRDD[K](rdd.rdds, getPartitionerWithNext(rdd, point)).
        setSerializer (p(rdd)('serializer)().asInstanceOf[Serializer])
      cgRdd
    case None =>
      rdd
  }
  
  /** adapt ShuffledRDD */
  private def adaptShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
      point: String, rdd: ShuffledRDD[K,V,C],
      prev: RDD[_ <: Product2[K,V]]) = apToActions.get (point) match {

    case Some(actions) =>

      implicit val keyOrderingOpt = p(rdd)('keyOrdering)().asInstanceOf[Option[Ordering[K]]]
      
      val shRdd = new ShuffledRDD[K,V,C](rdd.prev, getPartitionerWithNext(rdd, point)).
        setSerializer (
          p(rdd)('serializer)().asInstanceOf[Option[Serializer]].getOrElse(null)
        ).
        setKeyOrdering (keyOrderingOpt.getOrElse(null)).
        setAggregator (
          p(rdd)('aggregator)().asInstanceOf[Option[Aggregator[K,V,C]]].getOrElse(null)
        ).
        setMapSideCombine (
          p(rdd)('mapSideCombine)().asInstanceOf[Boolean]
        )
      shRdd

    case None =>
      rdd
  }

  //override def toString = s"OptHelper(${apToActions.map(kv => s"${kv._1}:${kv._2.mkString(",")}")})"
  override def toString = s"OptHelper(numActions=${apToActions.size})"
}

object OptHelper extends Logging {

  private var _instances: Map[SparkContext,OptHelper] = Map.empty

  def get(sc: SparkContext): OptHelper = _instances.get (sc) match {
    case Some(oh) =>
      oh

    case None =>
      val logPath = sc.getConf.get ("spark.adaptive.logpath", null)
      val oh = if (logPath == null || logPath.isEmpty) {
        new OptHelper
      } else {
        val analyzer = new Analyzer (logPath)
        new OptHelper (analyzer.getActions)
      }
      logInfo (s"${oh} created for ${sc}")
      _instances += (sc -> oh)
      oh
  }

  /**
   * Create an OptHelper directly from the discovered actions for each adaptive
   * point.
   */
  def apply(apToActions: Map[String,Seq[Action]]) = new OptHelper (apToActions)

  /**
   * Create an OptHelper from a string (python aux script)
   */
  def apply(actionsStr: String) = {

    var apToActions = Map[String,Seq[Action]]().
      withDefaultValue(Seq.empty)

    actionsStr.split(";").map (_.split(",")).foreach {
      case values if values(0) == "act-unp" =>
        apToActions += (values(1) -> (apToActions(values(1)) :+
          UNPAction(values(1), values(2).toInt)))

      case values if values(0) == "act-up" =>
        apToActions += (values(1) -> (apToActions(values(1)) :+
          UPAction(values(1), values(2))))

      case values if values(0) == "no-act" =>
        apToActions += (values(1) -> (apToActions(values(1))
          :+ NOAction(values(1))))

      case values =>
        throw new RuntimeException (s"Unrecognized Action: ${values(0)}")
    }

    new OptHelper(apToActions)
  }

}
