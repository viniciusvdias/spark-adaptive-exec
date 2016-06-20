package br.ufmg.cs.systems.sparktuner.model

import br.ufmg.cs.systems.sparktuner.Analyzer._

import org.json4s._
import org.json4s.native.JsonMethods._

class Stage(
    val id: Long,
    val name: String,
    val numTasks: Long,
    val tasks: Array[Task],
    val parents: Array[Long],
    val rdds: Array[RDD],
    onlyAdaptive: Boolean = false) {

  def this(jsonData: JValue) {
    this(
      getValue [Long] (jsonData, "Stage Info", "Stage ID"),
      getValue [String] (jsonData, "Stage Info", "Stage Name"),
      getValue [Long] (jsonData, "Stage Info", "Number of Tasks"),
      new Array (getValue [Long] (jsonData, "Stage Info", "Number of Tasks").toInt),
      getValue [Seq[BigInt]] (jsonData, "Stage Info", "Parent IDs").map (_.toLong).toArray,
      getJValue (jsonData, "Stage Info", "RDD Info").children.map (new RDD(_)).toArray
      )
  }

  def addTask(task: Task) = {
    tasks(task.idx.toInt) = task
  }

  def firstRDD: RDD = rdds.minBy (_.id)

  def emptyTasks: Array[Task] = tasks.filter (_.recordsRead == 0)

  def adaptivePoint: String = {
    val frdd = firstRDD
    if (firstRDD.name contains "adaptive")
      firstRDD.name
    else
      "-"
  }

  def canAdapt: Boolean =
    !onlyAdaptive || (adaptivePoint contains "adaptive")

  // statistics derived from tasks

  def taskRunTimes: Array[Long] = tasks.map (_.runTime)

  def taskGcTimes: Array[Long] = tasks.map (_.gcTime)
  
  def taskGcOverheads: Array[Double] = tasks.map (_.gcOverhead)

  def taskShuffleReadBytes: Array[Long] = tasks.map (_.shuffleReadBytes)

  def taskShuffleReadRecords: Array[Long] = tasks.map (_.shuffleReadRecords)

  def inputRecords: Long = tasks.map (_.inputRecords).sum

  def shuffleReadRecords: Long = tasks.map (_.shuffleReadRecords).sum

  def shuffleReadBytes: Long = tasks.map (_.shuffleReadBytes).sum

  def shuffleWriteRecords: Long = tasks.map (_.shuffleWriteRecords).sum

  def shuffleWriteBytes: Long = tasks.map (_.shuffleWriteBytes).sum

  def bytesSpilled: Long = tasks.map (_.bytesSpilled).sum

  def recordsRead: Long = inputRecords + shuffleReadRecords

  def taskGcTimesPerExecutor: Map[String,Seq[Long]] =
    tasks.map (t => (t.executorId, t.gcTime)).groupBy (_._1).mapValues (_.unzip._2)
  
  def taskGcOverheadsPerExecutor: Map[String,Seq[Double]] =
    tasks.map (t => (t.executorId, t.gcOverhead)).groupBy (_._1).mapValues (_.unzip._2)

  override def toString = {
    s"Stage(id=${id},numTasks=${tasks.size})"
  }

}

object Stage {
}
