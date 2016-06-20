package br.ufmg.cs.systems.sparktuner.model

import br.ufmg.cs.systems.sparktuner.Analyzer._

import org.json4s._
import org.json4s.native.JsonMethods._

case class RDD(
    val id: Long,
    val name: String,
    val parents: Array[Long],
    val callSite: String) {

  RDD.add (this)

  def this(jsonData: JValue) {
    this(
      getValue [Long] (jsonData, "RDD ID"),
      getValue [String] (jsonData, "Name"),
      getValue [Seq[BigInt]] (jsonData, "Parent IDs").map(_.toLong).toArray,
      getValue [String] (jsonData, "Callsite")
      )
  }

  override def equals(_that: Any) = _that match {
    case RDD(id, _, _, _) => this.id == id
    case _ => false
  }

  override def hashCode = (id, name).hashCode

  override def toString = {
    s"RDD(id=${id},name=${name},parentIds=${parents.mkString("[",",","]")},callsite=${callSite})"
  }
}

object RDD {
  var rdds: Map[Long,RDD] = Map.empty

  def add(rdd: RDD) = {
    rdds += (rdd.id -> rdd)
  }

  def get(rddId: Long) = rdds(rddId)
}
