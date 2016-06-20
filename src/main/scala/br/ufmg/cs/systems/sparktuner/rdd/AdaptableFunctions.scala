package br.ufmg.cs.systems.sparktuner.rdd

import br.ufmg.cs.systems.sparktuner.OptHelper
import br.ufmg.cs.systems.util.PrivateMethodExposer.p

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.PairRDDFunctions

import org.apache.spark.Partitioner
import java.lang.StackTraceElement

object AdaptableFunctions extends Logging {

  def preAdapt(sc: SparkContext) = {
    val optHelper = OptHelper.get (sc)
    logInfo (s"PreAdapt executed, result = ${optHelper}")
    optHelper
  }

  implicit def rdd2adaptableFunctions [K : ClassTag, V: ClassTag]
    (rdd: RDD[(K,V)]) = new AdaptableFunctionsKv(rdd)

  implicit def scAdaptableFuntions (sc: SparkContext) = 
    new AdaptableFunctionsSc(sc)

}

sealed trait AdaptableFunctions extends Logging with Serializable {

  def optHelper: OptHelper
  
  def getAdptName[T](_adptName: String): String = Option(_adptName) match {
    case Some(adptName) => adptName
    case None =>
      val stack = Thread.currentThread.getStackTrace()(1)
      s"${stack.getMethodName}:${stack.getLineNumber}"
  }
}

class AdaptableFunctionsSc (sc: SparkContext) extends AdaptableFunctions {
  
  override def optHelper: OptHelper = OptHelper.get (sc)

  def textFile(path: String,
      minPartitions: Int = sc.defaultMinPartitions): RDD[String] = {
    textFile (path, minPartitions, null)
  }

  def textFile(
      path: String,
      minPartitions: Int,
      _adptName: String): RDD[String] = {
    val adptName = getAdptName (_adptName)
    sc.textFile (path, optHelper.getNumPartitions(minPartitions, adptName)).
      setName (adptName)
  }
}

class AdaptableFunctionsKv [K,V] (self: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctions[K,V](self) with AdaptableFunctions {

  override def optHelper: OptHelper = OptHelper.get (self.sparkContext)
 
  /** override default functions to force adaptative behavior **/

  override def reduceByKey (func: (V, V) => V): RDD[(K, V)] =
    reduceByKey (func, null)

  override def reduceByKey (func: (V, V) => V, numPartitions: Int): RDD[(K,V)] =
    reduceByKey (func, numPartitions, null)
  
  override def join [W] (other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))] = {
    join (other, numPartitions, null)
  }

  /******/

  def reduceByKey (func: (V, V) => V, _adptName: String): RDD[(K, V)] = {
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self)
    self.reduceByKey(partitioner, func).setName(adptName)
  }

  def reduceByKey (func: (V, V) => V, numPartitions: Int,
      _adptName: String): RDD[(K, V)] = {
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self,
      defaultNumPartitions = numPartitions)
    self.reduceByKey(partitioner, func).setName (adptName)
  }

  def aggregateByKey [U: ClassTag] (zeroValue: U, numPartitions: Int, _adptName: String)(
      seqOp: (U, V) => U, combOp: (U, U) => U): RDD[(K, U)] = {
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self,
      defaultNumPartitions = numPartitions)
    self.aggregateByKey (zeroValue, partitioner)(seqOp, combOp).setName (adptName)
  }

  def partitionBy(partitioner: Partitioner, _adptName: String): RDD[(K, V)] = {
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self)
    self.partitionBy (partitioner).setName (adptName)
  }

  def join [W] (other: RDD[(K, W)], numPartitions: Int,
      _adptName: String): RDD[(K, (V, W))] = {
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self,
      defaultNumPartitions = numPartitions)
    self.join (other, partitioner).setName (adptName)
  }

  def join [W] (other: RDD[(K, W)], _adptName: String): RDD[(K, (V, W))] = {
    val adptName = getAdptName (_adptName)
    val partitioner = optHelper.getPartitioner (adptName, prev = self)
    self.join (other, partitioner).setName (adptName)
  }

}
