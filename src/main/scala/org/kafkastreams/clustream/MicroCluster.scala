package org.kafkastreams.clustream

import java.util.concurrent.atomic.AtomicLong

import breeze.linalg.{Vector, sum}
import breeze.stats.distributions.Gaussian
import org.kafkastreams.clustream.mcInfo.McInfo


/**
  *
  * @param cf2x
  * @param cf1x
  * @param cf1t
  * @param n
  * @param ids
  */
class MicroCluster(
                              var cf2x:Vector[Double],
                              var cf1x:Vector[Double],
                              var cf2t:Long,
                              var cf1t:Long,
                              var n:Long,
                              var ids:Array[Int],
                              var rmsd:Double= 0.0) extends Serializable{
  var center: Vector[Double] = cf1x :/ n.toDouble

  def this(cf2x:Vector[Double],cf1x:Vector[Double],cf2t:Long,cf1t:Long,n:Long) = this(cf2x,cf1x,cf2t,cf1t,n,Array(MicroCluster.inc))
  def this(cf2x:Vector[Double],cf1x:Vector[Double]) = this(cf2x,cf1x,0,0,0,Array(MicroCluster.globalInc))

  def addPoint(point :Vector[Double],time:AtomicLong) : Unit = {
    setCf1x(cf1x :+ point)
    setCf2x(cf2x :+ (point :* point))
    val currentTime = time.get()
    setCf1t(cf1t + currentTime)
    setCf2t(cf2t + currentTime*currentTime)
    setN(n + 1L)
    setCenter(cf1x :/ n.toDouble)
    setRmsd(scala.math.sqrt(sum(this.cf2x) / this.n.toDouble - sum(this.cf1x.map(a => a * a)) / (this.n * this.n.toDouble)))
  }
  def addPoint(mcInfo:McInfo,gtime : AtomicLong) : Unit = {
    setCf1x(cf1x :+ Vector(mcInfo.getCf1x.split(",").map(_.toDouble)))
    setCf2x(cf2x :+ Vector(mcInfo.getCf2x.split(",").map(_.toDouble)))
    //gtime在每次中间结果被发送时递增1
    val currentTime = gtime.longValue()
    setCf1t(cf1t + currentTime)
    setCf2t(cf2t + currentTime*currentTime)
    setN(n + mcInfo.getN)
    setCenter(cf1x :/ n.toDouble)
    setRmsd(scala.math.sqrt(sum(this.cf2x) / this.n.toDouble - sum(this.cf1x.map(a => a * a)) / (this.n * this.n.toDouble)))
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
      "n:  "+n+" || "+
      "Cf1x:  "+cf1x.toString+" || "+
      "Cf2x:  "+cf2x.toString+" || "+
      "Cf1t:  "+cf1t.toString+" || "+
      "Cf2t:  "+cf2t.toString+" || "+
      "Centroid:  "+center.toString+" || "+
      "Ids:  "+ ids.mkString("{",",","}") + "\n"
  }
}

private object MicroCluster extends Serializable {
  private var current = -1
  private var globalCurrent = 9

  private def inc = {
    current += 1
    current
  }

  private def globalInc = {
    globalCurrent += 1
    globalCurrent
  }
}



