package br.ufmg.cs.systems.sparktuner.model

import br.ufmg.cs.systems.sparktuner.Analyzer._

import org.json4s._
import org.json4s.native.JsonMethods._

class Task(
    // general
    val stageId: Long,
    val idx: Long,
    val executorId: String,
    val runTime: Long,
    val gcTime: Long,
    val inputRecords: Long,
    // spill
    val memoryBytesSpilled: Long,
    val diskBytesSpilled: Long,
    // shuffles
    val shuffleReadRecords: Long, val shuffleReadBytes: Long,
    val shuffleWriteRecords: Long, val shuffleWriteBytes: Long) {

  /**
   * TODO: make static definitions for the following keys ??
   */
  def this(taskData: JValue) {
    this (
      getValue [Long] (taskData, "Stage ID"),
      getValue [Long] (taskData, "Task Info", "Index"),
      getValue [String] (taskData, "Task Info", "Executor ID"),
      getValue [Long] (taskData, "Task Metrics", "Executor Run Time"),
      getValue [Long] (taskData, "Task Metrics", "JVM GC Time"),
      getValue [Long] (taskData, "Task Metrics", "Input Metrics", "Records Read"),
      getValue [Long] (taskData, "Task Metrics", "Memory Bytes Spilled"),
      getValue [Long] (taskData, "Task Metrics", "Disk Bytes Spilled"),
      getValue [Long] (taskData, "Task Metrics", "Shuffle Read Metrics", "Total Records Read"),
      getValue [Long] (taskData, "Task Metrics", "Shuffle Read Metrics", "Local Bytes Read") +
        getValue [Long] (taskData, "Task Metrics", "Shuffle Read Metrics", "Remote Bytes Read"),
      getValue [Long] (taskData, "Task Metrics", "Shuffle Write Metrics", "Shuffle Records Written"),
      getValue [Long] (taskData, "Task Metrics", "Shuffle Write Metrics", "Shuffle Bytes Written")
      )
  }

  def recordsRead = inputRecords + shuffleReadRecords

  def bytesSpilled = memoryBytesSpilled + diskBytesSpilled

  // smaller the better
  def gcOverhead: Double = gcTime / runTime.toDouble

  override def toString = {
    s"Task(idx=${idx},runtime=${runTime},gcTime=${gcTime},inputRecords=${inputRecords}" +
    s",shuffleReadRecords=${shuffleReadRecords},shuffleReadBytes=${shuffleReadBytes}" +
    s",shuffleWriteRecords=${shuffleWriteRecords},shuffleWriteBytes=${shuffleWriteBytes}" +
    ")"
  }
}
