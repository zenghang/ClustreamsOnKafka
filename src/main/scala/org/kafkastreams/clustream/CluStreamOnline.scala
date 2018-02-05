package org.kafkastreams.clustream

import breeze.linalg._
import org.rocksdb.Experimental
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, Produced}
import org.kafkastreams.implicits.KeyValueImplicits
import java.lang

import breeze.stats.distributions.Gaussian
/**
  * Created by 11245 on 2018/1/19.
  */

class CluStreamOnline(
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


  var initialized = false

  private var initArr: Array[Vector[Double]] = Array()

  /**
    *
    * @param kStreamData
    */
  private def initKmeans(kStreamData: KStream[String,Vector[Double]]):Unit = {

  }

  /**
    * @define 随机初始化分布式聚类的类簇
    *
    */
  private def initRandom = {
    var i =1;
    for(mc <- microClusters){
      mc.setCenter(Vector.fill[Double](numDimensions)(rand()))
      mc.setIds(Array(i))
      i+=1
    }
    initialized = true
  }

  /**
    *
    * @param data
    */
  def run(data:KStream[String,String]):Unit = {
    //currentN = data  时间的设计

      initRandom
      updateMicroClusters(data)

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
  private def distanceNearestMC(vec: Vector[Double], mcs: Array[MicroCluster]): Double = {

    var minDist = Double.PositiveInfinity
    var i = 0
    for (mc <- mcs) {
      val dist = squaredDistance(vec, mc.getCenter)
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


  }

  /**
    * @define 找到距离某数据点最近的类簇
    */
  private def findNearestMicoCluster(point : Vector[Double]) : MicroCluster = {
    var nearMC: MicroCluster = null
    var minDist = Double.PositiveInfinity
    for (mc <- microClusters) {

      val dist = squaredDistance(mc.getCenter, point)
      if (dist < minDist) {
        minDist = dist
        nearMC = mc
      }
    }
    nearMC
  }

  /**
    *
    * @param point
    * @return 返回被删除的类簇的索引，如没有则返回-1
    * @define 找到需要被删除的类簇并用新的数据点新建一个类簇
    */
  private def deleteAndReplaceMicoCluster(point : Vector[Double]) : Int = {
    var DeletedIndex = -1
    val recencyThreshold = this.time - delta
    for(i <- 0 until q){
      if (microClusters(i).getMTimeStamp(mLastPoints) < recencyThreshold || microClusters(i).getN == 0){
        val ids = microClusters(i).getIds
        microClusters(i) = new MicroCluster(point :* point, point, this.time * this.time, this.time, 1L)
        microClusters(i).setCenter(point)
        //暂时设为原来的id
        microClusters(i).setIds(ids)
        microClusters(i).setRmsd(distanceNearestMC(point, microClusters))
        return i
      }
    }
    DeletedIndex
  }

  private def mergeAndReplaceMicroCluster(point : Vector[Double]) : Int = {
    //找到距离最近的两个类簇
    var closestA = 0
    var closestB = 0
    var minDist = Double.PositiveInfinity
    for (i <- 0 until q-1) {
      val centA = microClusters(i).getCenter
      for(j <-i+1 until q) {
        val dist = squaredDistance(centA,microClusters(j).getCenter)
        if(dist < minDist) {
          minDist = dist
          closestA = i
          closestB = j
        }
      }
    }

    mergeMicroClusters(closestA,closestB)
    val ids = microClusters(closestB).getIds
    microClusters(closestB) = new MicroCluster(point :* point, point, this.time * this.time, this.time, 1L)
    microClusters(closestB).setCenter(point)
    //暂时设为原来的id
    microClusters(closestB).setIds(ids)
    microClusters(closestB).setRmsd(distanceNearestMC(point, microClusters))

    closestB

  }

  import KeyValueImplicits._

  /**
    *
    * @param dataPoints
    */
  private def updateMicroClusters(dataPoints:KStream[String,String]): Unit = {
//    var mcStream: KStream[Array[Double],MicroCluster] = null
//    if(initialized){
//      mcStream = dataPoints.map{
//        (key,valueString) =>
//          val value = Vector(valueString.split(",").map(_.toDouble))
//          var finalMC = null
//          // 1.找到最近的类簇
//          val nearestMC = findNearestMicoCluster(value)
//          val minDistance = Math.sqrt(squaredDistance(nearestMC.getCenter, value))
//            //如果在类簇半径范围内，则将点添加进去
//            if(minDistance <= tFactor * nearestMC.rmsd) {
//              nearestMC.addPoint(value,this.time)
//              nearestMC.setRmsd(distanceNearestMC(nearestMC.getCenter,microClusters))
//              this.time += 1L
//              //打印此类簇（测试用）
//              print(nearestMC)
//            }
//            else {
//              //2.查找是否有符合删除条件的类簇，如果有则删除并以数据点为中心新建一个类簇替换
//              val DeletedIndex : Int = deleteAndReplaceMicoCluster(value)
//
//              //3.如果没有可删除的类簇，则进行合并，然后以数据点为中心新建一个类簇
//              if(DeletedIndex == -1) {
//                val MergedIndex : Int = mergeAndReplaceMicroCluster(value)
//                //打印类簇（测试用）
//                print(microClusters(MergedIndex))
//              }
//              //打印类簇（测试用）
//              print(microClusters(DeletedIndex))
//            }
//
//
//          (microClusters(1).getCenter.toArray,microClusters(1))
//      }
//    }
    dataPoints.foreach{ (key,valueString) =>

        val value = Vector(valueString.split(",").map(_.toDouble))
        var finalMC = null
        // 1.找到最近的类簇
        val nearestMC = findNearestMicoCluster(value)
        val minDistance = Math.sqrt(squaredDistance(nearestMC.getCenter, value))
        //如果在类簇半径范围内，则将点添加进去
        if(minDistance <= tFactor * nearestMC.rmsd) {
          nearestMC.addPoint(value,this.time)
          nearestMC.setRmsd(distanceNearestMC(nearestMC.getCenter,microClusters))
          this.time += 1L
          //打印此类簇（测试用）
          print(nearestMC)
        }
        else {
          //2.查找是否有符合删除条件的类簇，如果有则删除并以数据点为中心新建一个类簇替换
          val DeletedIndex : Int = deleteAndReplaceMicoCluster(value)

          //3.如果没有可删除的类簇，则进行合并，然后以数据点为中心新建一个类簇
          if(DeletedIndex == -1) {
            val MergedIndex : Int = mergeAndReplaceMicroCluster(value)
            //打印类簇（测试用）
            print(microClusters(MergedIndex))
          }
          //打印类簇（测试用）
          print(microClusters(DeletedIndex))
        }
    }
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
                              var cf2x:Vector[Double],
                              var cf1x:Vector[Double],
                              var cf2t:Long,
                              var cf1t:Long,
                              var n:Long,
                              var ids:Array[Int],
                              var rmsd:Double= 0.0) extends Serializable{
  var center: Vector[Double] = cf1x :/ n.toDouble

  def this(cf2x:Vector[Double],cf1x:Vector[Double],cf2t:Long,cf1t:Long,n:Long) = this(cf2x,cf1x,cf2t,cf1t,n,Array(MicroCluster.inc))

  def addPoint(point :Vector[Double],time:Long) : Unit = {
    setCf1x(cf1x :+ point)
    setCf2x(cf2x :+ (point :* point))
    setCf1t(cf1t + time)
    setCf2t(cf2t + time*time)
    setN(n + 1L)
    setCenter(cf1x :/ n.toDouble)
  }

  def getMTimeStamp(mLastPoints :Int) = {
    var mTimeStamp: Double = 0.0
    val meanTimeStamp = if (n > 0) cf1t.toDouble / n.toDouble else 0
    val sdTimeStamp = scala.math.sqrt(cf2t.toDouble / n.toDouble - meanTimeStamp * meanTimeStamp)

    if (n < 2 * mLastPoints) mTimeStamp = meanTimeStamp
    else mTimeStamp = Gaussian(meanTimeStamp, sdTimeStamp).icdf(1 - mLastPoints / (2 * n.toDouble))
    mTimeStamp
  }

  def setCf2x(cf2x: Vector[Double]):Unit = {
    this.cf2x = cf2x
  }

  def getCf2x:Vector[Double] = {
    this.cf2x
  }

  def setCf1x(cf1x:Vector[Double]):Unit = {
    this.cf1x = cf1x
  }

  def getCf1x: Vector[Double] = {
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

  def getCenter: Vector[Double] = {
    this.center
  }

  def setCenter(center:Vector[Double]): Unit = {
    this.center = center
  }

  def setRmsd(rmsd: Double): Unit = {
    this.rmsd = rmsd
  }

  override def toString = {
      "n:  "+n+"\n"+
      "Cf1x:  "+cf1x.toString+"\n"+
      "Cf2x:  "+cf2x.toString+"\n"+
      "Cf1t:  "+cf1t.toString+"\n"+
      "Cf2t:  "+cf2t.toString+"\n"+
      "Centroid:  "+center.toString+"\n"
  }
}


/**
  *
  * @param centroid
  * @param rmsd
  * @param n
  */
private class MicroClusterInfo(
                                var centroid: Vector[Double],
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


