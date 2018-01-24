package org.kafkastreams.clustream

import breeze.linalg._
import org.rocksdb.Experimental
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, Produced}
import org.kafkastreams.keyvalue.KeyValueImplicits
import java.lang
/**
  * Created by 11245 on 2018/1/19.
  */
@Experimental
class MapFunctionScalaExample(
                               val q:Int,
                               val numDimensions:Int,
                               val minInitPoints:Int)
  extends Serializable{


  def timer[R](block: => R):R = {
    val t0 = System.nanoTime()
    val result = block//call-by-name
    val t1 = System.nanoTime()
    result
  }
  private var mLastPoints = 500
  private var delta = 20
  private var tFactor = 2.0
  private var recursiveOutliersRMSDCheck = true

  private var time:Long = 0L
  private var N: Long = 0L
  private var currentN: Long = 0L

  private var microClusters: Array[MicroCluster] = Array.fill(q)(new MicroCluster(Vector.fill[Double](numDimensions)(0.0), Vector.fill[Double](numDimensions)(0.0), 0L, 0L, 0L))
  private var mcInfo: Array[(MicroClusterInfo, Int)] = null

  var initialized = false

  private var initArr: Array[breeze.linalg.Vector[Double]] = Array()

  /**
    *
    * @param kStreamData
    */
  private def initKmeans(kStreamData: KStream[String,breeze.linalg.Vector[Double]]):Unit = {

  }

  /**
    *
    * @param data
    */
  def run(data:KStream[String,breeze.linalg.Vector[Double]]):Unit = {
    //currentN = data  时间的设计
    if(initialized){
      val assignations = assignToMicroCluster(data)
      //updateMicroClusters(assignations)

      var i = 0
      for (mc <- microClusters) {
        mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
        if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x :/ mc.n.toDouble)
        mcInfo(i)._1.setN(mc.getN)
        if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
        i += 1
      }
      for (mc <- mcInfo) {
        if (mc._1.n == 1)
          mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
      }
      //把新的簇信息发送到一个公共的topic
    }else{
      initKmeans(data)
    }
    //
  }

  /**
    *
    * @return
    */
  def getMicroClusters: Array[MicroCluster] = {
    this.microClusters
  }

  def getCurrentTime: Long = {
    this.time
  }

  def getTotalPoints: Long = {
    this.N
  }

  def setRecursiveOutliersRMSDCheck(ans: Boolean): this.type = {
    this.recursiveOutliersRMSDCheck = ans
    this
  }

  def setM(m: Int): this.type = {
    this.mLastPoints = m
    this
  }

  def setDelta(d: Int): this.type = {
    this.delta = d
    this
  }

  def setTFactor(t: Double): this.type = {
    this.tFactor = t
    this
  }

  /**
    *
    * @param vec
    * @param mcs
    * @return
    */
  private def distanceNearestMC(vec: breeze.linalg.Vector[Double], mcs: Array[(MicroClusterInfo, Int)]): Double = {

    var minDist = Double.PositiveInfinity
    var i = 0
    for (mc <- mcs) {
      val dist = squaredDistance(vec, mc._1.centroid)
      if (dist != 0.0 && dist < minDist) minDist = dist
      i += 1
    }
    scala.math.sqrt(minDist)
  }

  /**
    *
    * @param idx1
    * @param idx2
    * @return
    */
  private def squaredDistTwoMCArrIdx(idx1: Int, idx2: Int): Double = {
    squaredDistance(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble, microClusters(idx2).getCf1x :/ microClusters(idx2).getN.toDouble)
  }

  /**
    *
    * @param idx1
    * @param point
    * @return
    */
  private def squaredDistPointToMCArrIdx(idx1: Int, point: Vector[Double]): Double = {
    squaredDistance(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble, point)
  }

  /**
    *
    * @param idx0
    * @return
    */
  private def getArrIdxMC(idx0: Int): Int = {
    var id = -1
    var i = 0
    for (mc <- microClusters) {
      if (mc.getIds(0) == idx0) id = i
      i += 1
    }
    id
  }

  /**
    *
    * @param idx1
    * @param idx2
    */
  private def mergeMicroClusters(idx1: Int, idx2: Int): Unit = {

    microClusters(idx1).setCf1x(microClusters(idx1).getCf1x :+ microClusters(idx2).getCf1x)
    microClusters(idx1).setCf2x(microClusters(idx1).getCf2x :+ microClusters(idx2).getCf2x)
    microClusters(idx1).setCf1t(microClusters(idx1).getCf1t + microClusters(idx2).getCf1t)
    microClusters(idx1).setCf2t(microClusters(idx1).getCf2t + microClusters(idx2).getCf2t)
    microClusters(idx1).setN(microClusters(idx1).getN + microClusters(idx2).getN)
    microClusters(idx1).setIds(microClusters(idx1).getIds ++ microClusters(idx2).getIds)

    mcInfo(idx1)._1.setCentroid(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble)
    mcInfo(idx1)._1.setN(microClusters(idx1).getN)
    mcInfo(idx1)._1.setRmsd(scala.math.sqrt(sum(microClusters(idx1).cf2x) / microClusters(idx1).n.toDouble - sum(microClusters(idx1).cf1x.map(a => a * a)) / (microClusters(idx1).n * microClusters(idx1).n.toDouble)))

  }

  /**
    *
    * @param idx1
    * @param point
    */
  private def addPointMicroClusters(idx1: Int, point: Vector[Double]): Unit = {

    microClusters(idx1).setCf1x(microClusters(idx1).getCf1x :+ point)
    microClusters(idx1).setCf2x(microClusters(idx1).getCf2x :+ (point :* point))
    microClusters(idx1).setCf1t(microClusters(idx1).getCf1t + this.time)
    microClusters(idx1).setCf2t(microClusters(idx1).getCf2t + (this.time * this.time))
    microClusters(idx1).setN(microClusters(idx1).getN + 1)

    mcInfo(idx1)._1.setCentroid(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble)
    mcInfo(idx1)._1.setN(microClusters(idx1).getN)
    mcInfo(idx1)._1.setRmsd(scala.math.sqrt(sum(microClusters(idx1).cf2x) / microClusters(idx1).n.toDouble - sum(microClusters(idx1).cf1x.map(a => a * a)) / (microClusters(idx1).n * microClusters(idx1).n.toDouble)))

  }

  /**
    *
    * @param idx
    * @param point
    */
  private def replaceMicroCluster(idx: Int, point: Vector[Double]): Unit = {
    microClusters(idx) = new MicroCluster(point :* point, point, this.time * this.time, this.time, 1L)
    mcInfo(idx)._1.setCentroid(point)
    mcInfo(idx)._1.setN(1L)
    mcInfo(idx)._1.setRmsd(distanceNearestMC(mcInfo(idx)._1.centroid, mcInfo))
  }

  import KeyValueImplicits._
  /**
    *
    * @param kStreamData
    * @param mcInfo
    * @return
    */
  private def assignToMicroCluster(kStreamData: KStream[String,breeze.linalg.Vector[Double]], mcInfo: Array[(MicroClusterInfo, Int)]){

    val minClusterIndex:KStream[Int,breeze.linalg.Vector[Double]] = kStreamData.map{(key,value) =>
      var minDist = Double.PositiveInfinity
      var minIndex = Int.MaxValue
      var i = 0
      for (mc <- mcInfo) {
        val dist = squaredDistance(value,mc._1.centroid)
        if (dist < minDist) {
          minDist = dist
          minIndex = mc._2
        }
        i += 1
      }
      (minIndex,value)
    }
    minClusterIndex
  }

  private def assignToMicroCluster(data:KStream[String,breeze.linalg.Vector[Double]]):KStream[Int,breeze.linalg.Vector[Double]] = {
    data.map { (key,value) =>
      var minDist = Double.PositiveInfinity
      var minIndex = Int.MaxValue
      var i = 0
      //从topic里面取簇的信息，计算最近的簇的标签
      (minIndex, value)
    }
  }

  /**
    *
    * @param assignations
    */
  private def updateMicroClusters(assignations:KStream[(Int,Vector[Double])]): Unit = {
    var dataInAndOut: KStream[(Int, (Int, Vector[Double]))] = null
    var dataIn: KStream[(Int, Vector[Double])] = null
    var dataOut: KStream[(Int, Vector[Double])] = null
    //1.Calculate RMSD
    if (initialized) {
      //从topic里面拿簇的信息，计算点到最近簇的距离，将距离与半径对比，分为在簇内和簇外
    }
    //2.Separate data

  }
}

/**
  *
  */
private object MicroCluster extends Serializable {
  private var current = -1

  private def inc = {
    current += 1
    current
  }
}

/**
  *
  * @param cf2x
  * @param cf1x
  * @param cf1t
  * @param n
  * @param ids
  */
protected class MicroCluster(
                              var cf2x:breeze.linalg.Vector[Double],
                              var cf1x:breeze.linalg.Vector[Double],
                              var cf2t:Long,
                              var cf1t:Long,
                              var n:Long,
                              var ids:Array[Int]) extends Serializable{
  def this(cf2x:breeze.linalg.Vector[Double],cf1x:breeze.linalg.Vector[Double],cf2t:Long,cf1t:Long,n:Long) = this(cf2x,cf1x,cf2t,cf1t,n,Array(MicroCluster.inc))

  def setCf2x(cf2x: breeze.linalg.Vector[Double]):Unit = {
    this.cf2x = cf2x
  }

  def getCf2x:breeze.linalg.Vector[Double] = {
    this.cf2x
  }

  def setCf1x(cf1x:breeze.linalg.Vector[Double]):Unit = {
    this.cf1x = cf1x
  }

  def getCf1x: breeze.linalg.Vector[Double] = {
    this.cf1x
  }

  def setCf2t(cf2t: Long): Unit = {
    this.cf2t = cf2t
  }

  def getCf2t: Long = {
    this.cf2t
  }

  def setCf1t(cf1t: Long): Unit = {
    this.cf1t = cf1t
  }

  def getCf1t: Long = {
    this.cf1t
  }

  def setN(n: Long): Unit = {
    this.n = n
  }

  def getN: Long = {
    this.n
  }

  def setIds(ids: Array[Int]): Unit = {
    this.ids = ids
  }

  def getIds: Array[Int] = {
    this.ids
  }
}


/**
  *
  * @param centroid
  * @param rmsd
  * @param n
  */
private class MicroClusterInfo(
                                var centroid: breeze.linalg.Vector[Double],
                                var rmsd:Double,
                                var n:Long)extends Serializable{
  def setCentroid(centroid:Vector[Double]): Unit ={
    this.centroid = centroid
  }
  def setRmsd(rmsd:Double):Unit = {
    this.rmsd = rmsd
  }
  def setN(n:Long):Unit = {
    this.n = n
  }
}


