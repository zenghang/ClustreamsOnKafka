package org.kafkastreams.clustream

import java.io.{File, FileOutputStream, IOException, ObjectOutputStream}
import java.nio.file.{Files, Paths}
import java.util
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import java.util.{Collections, Properties}

import breeze.linalg._
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.KStream
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.kafkastreams.clustream.mcInfo.McInfo

/**
  * Created by 11245 on 2018/1/19.
  */

class CluStreamOnline(
                               val q:Int,
                               val numDimensions:Int,
                               val minInitPoints:Int)
  extends Serializable{



  //cluOnline类标记（测试用）
  private val Cluid : Double = rand()

  private var mLastPoints = 500
  private var delta = 20
  private var tFactor = 2.0
  private var recursiveOutliersRMSDCheck = true

  private val time : AtomicLong = new AtomicLong(0)
  private val gtime : AtomicLong = new AtomicLong(0)
  private var N: Long = 0L
  private var initNum: Int = 0

  private var microClusters: Array[MicroCluster] = Array.fill(q)(new MicroCluster(Vector.fill[Double](numDimensions)(0.0), Vector.fill[Double](numDimensions)(0.0), 0L, 0L, 0L))


  var initialized = false
  var sendClusterToTopic = false
  private var initialClusters : Array[kmeansModel] = Array.fill(q)(new kmeansModel(Vector.fill[Double](numDimensions)(0.0),1))
  /**
    *
    * @param point
    */
  def initKmeans(point:Vector[Double]):Unit = {
    if(sendClusterToTopic){
      microClusters = Array.fill(q)(new MicroCluster(Vector.fill[Double](numDimensions)(0.0), Vector.fill[Double](numDimensions)(0.0), 0L, 0L, 0L))
      initialClusters = Array.fill(q)(new kmeansModel(Vector.fill[Double](numDimensions)(0.0),1))
      initNum = 0;
      sendClusterToTopic = false
    }
    //1.前q个点用来初始化(最开始选的q个中心点)
    if(initNum < q){
      initialClusters(initNum).setCf1x(point)
      initialClusters(initNum).setCenter(point)
      initNum += 1
    }
    else if(initNum < minInitPoints){
      //1.计算point到每个中心点
      var minDist = Double.PositiveInfinity
      var minIndex = 0
      for(i <- 0 until microClusters.length - 1){
        val dist = squaredDistance(initialClusters(i).getCenter, point)
        if (dist < minDist) {
          minDist = dist
          minIndex = i
        }
      }
      //2.更新新的中心点
      initialClusters(minIndex).setCf1x(initialClusters(minIndex).getCf1x:+point)
      initialClusters(minIndex).n += 1
      initialClusters(minIndex).setCenter(initialClusters(minIndex).getCf1x / initialClusters(minIndex).n.toDouble)
      initNum += 1
    }
    if(initNum == minInitPoints){
      //将minInitPoints个点后得到的中心赋值给microClusters的中心用作初始化
      this.time.set(0);
      var i =1;
      for(mc <- microClusters){
        mc.setCenter(initialClusters(i-1).getCenter)
        mc.setIds(Array(i))
        i+=1
      }
      for(mc <- microClusters){
        mc.setRmsd(distanceNearestMC(mc.center, microClusters))
      }
      initialized = true
    }
  }

  /**
    * @define 随机初始化分布式聚类的类簇
    *
    */
   def initRandom = {
    this.time.set(0)
    var i =1
    for(mc <- microClusters){
      mc.setCenter(Vector.fill[Double](numDimensions)(scala.util.Random.nextInt(100).toString.toDouble))
      mc.setIds(Array(i))
      i+=1
    }
    for(mc <- microClusters)
      mc.setRmsd(distanceNearestMC(mc.center, microClusters))
    initialized = true
  }


  /**
    *
    * @param data
    */
  def run(data:Vector[Double]):Unit = {
    if (initialized) {
      updateMicroClusters(data)
    } else {
      initRandom
      updateMicroClusters(data)
    }

    
  }
  def globalrun(data:Vector[Double],mcInfo:McInfo):Unit = {
    if(initialized){
      globalupadateMicroClusters(data,mcInfo)
    }else{
      initRandom
      globalupadateMicroClusters(data,mcInfo)
    }
  }
  /**
    *
    * @return
    */
  def getMicroClusters: Array[MicroCluster] = {
    this.microClusters
  }

  def getCurrentTime: Long = {
    this.time.longValue()
  }

  def getGlobalTime: Long = {
    this.gtime.longValue()
  }

  def getAtomicTime: AtomicLong = {
    this.time
  }

  def getAtomicGlobalTime: AtomicLong = {
    this.gtime
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
    * 返回可以安全删除的索引
    * @return
    */
  private def markDeleteMicroCluster() : Int = {
    var DeletedIndex = -1
    val recencyThreshold = this.time.longValue() - delta
    for(i <- 0 until q){
      if (microClusters(i).getMTimeStamp(mLastPoints) < recencyThreshold || microClusters(i).getN == 0){
        return i
      }
    }
    DeletedIndex
  }



  /**
    * 合并最近的两个簇返回消失的最近簇的索引
    * @return
    */
  private def markAndMergeMicroCluster() : Int ={
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
    closestB
  }
  private def ReplaceMicroCluster(point : Vector[Double],replacedID : Int) : Int = {
    microClusters(replacedID) = new MicroCluster(Vector.fill[Double](numDimensions)(0.0), Vector.fill[Double](numDimensions)(0.0),0L,0L,0L)
    microClusters(replacedID).addPoint(point,this.time)
    microClusters(replacedID).setCenter(point)
    microClusters(replacedID).setRmsd(distanceNearestMC(point, microClusters))

    replacedID
  }

  private def ReplaceMicroCluster(point : Vector[Double],replacedID : Int,mcInfo:McInfo) : Unit = {
    microClusters(replacedID) = new MicroCluster(Vector.fill[Double](numDimensions)(0.0), Vector.fill[Double](numDimensions)(0.0),0L,0L,0L)
    microClusters(replacedID).addPoint(mcInfo,this.gtime)
    microClusters(replacedID).setCenter(point)
    microClusters(replacedID).setRmsd(distanceNearestMC(point, microClusters))

  }


  def arrayToString(array: Array[Double]) : String = {

    var centerString : String = ""
    for (i <- 0 until array.length) {
      centerString += array(i).toString
      if (i != array.length - 1)
        centerString += ","
    }
    centerString

  }

  def sendClustersToTopic(topic : String,schemaRegistryUrl:String,producerConfig : Properties): Unit ={
    //设置序列化器
    val serdeConfig : util.Map[String, String] = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    val McInfoSerde : SpecificAvroSerde[McInfo] = new SpecificAvroSerde[McInfo]()
    McInfoSerde.configure(serdeConfig, false)

    val mcProducer : KafkaProducer[String,McInfo] = new KafkaProducer[String,McInfo](producerConfig
      ,Serdes.String().serializer(),McInfoSerde.serializer())

    for (mc <- microClusters) {
      if(mc.n > 0) {
        val centroid : String= arrayToString(mc.center.toArray)
        val cf1xString : String = arrayToString(mc.cf1x.toArray)
        val cf2xString : String = arrayToString(mc.cf2x.toArray)
        val mcInfo : McInfo =  new McInfo(mc.n,cf1xString,cf2xString,mc.cf1t,mc.cf2t)

        //打印信息测试用
        println(mc.toString)

        //立即初始化单个类簇
        mc.setN(0);
        mc.setCf1x(Vector.fill[Double](numDimensions)(0.0))
        mc.setCf2x(Vector.fill[Double](numDimensions)(0.0))
        mc.setCf1t(0)
        mc.setCf2t(0)


        mcProducer.send(new ProducerRecord[String,McInfo](topic,centroid,mcInfo))

        sendClusterToTopic = true

      }
    }
  }


  private def updateMicroClusters(value:Vector[Double]): Unit = {

        var finalMC = null
        // 1.找到最近的类簇
        val nearestMC = findNearestMicoCluster(value)
        val minDistance = Math.sqrt(squaredDistance(nearestMC.getCenter, value))
        //如果在类簇半径范围内，则将点添加进去
        if(minDistance <= tFactor * nearestMC.rmsd) {
          nearestMC.addPoint(value,this.time)
          //打印此类簇（测试用）
          //print(nearestMC)
        }
        else {
          //2.查找是否有符合删除条件的类簇，如果有则删除并以数据点为中心新建一个类簇替换
          val DeletedIndex : Int = markDeleteMicroCluster

          //3.如果没有可删除的类簇，则进行合并，然后以数据点为中心新建一个类簇
          if(DeletedIndex == -1) {
            val MergedIndex : Int = markAndMergeMicroCluster
            ReplaceMicroCluster(value,MergedIndex)
            //打印类簇（测试用）
            //print(microClusters(MergedIndex))
          }else{
            ReplaceMicroCluster(value,DeletedIndex)
            //打印类簇（测试用）
            //print(microClusters(DeletedIndex))
          }
        }
  }
  private def globalupadateMicroClusters(data:Vector[Double],mcInfo:McInfo): Unit = {
    //1.找到最近的簇
    val nearestMC = findNearestMicoCluster(data)
    val minDistance = Math.sqrt(squaredDistance(nearestMC.getCenter, data))
    //如果在类簇半径范围内，则将点添加进去
    if(minDistance <= tFactor * nearestMC.rmsd) {
      nearestMC.addPoint(mcInfo,this.gtime)
      //打印此类簇（测试用）
      //print(nearestMC)
    }else{
      //2.查找是否有符合删除条件的类簇，如果有则删除并以数据点为中心新建一个类簇替换
      val DeletedIndex : Int = markDeleteMicroCluster

      //3.如果没有可删除的类簇，则进行合并，然后以数据点为中心新建一个类簇
      if(DeletedIndex == -1) {
        val MergedIndex : Int = markAndMergeMicroCluster()
        ReplaceMicroCluster(data,MergedIndex,mcInfo)
        //打印类簇（测试用）
        //print(microClusters(MergedIndex))
      }else{
        ReplaceMicroCluster(data,DeletedIndex,mcInfo)
        //打印类簇（测试用）
        //print(microClusters(DeletedIndex))
      }
    }

  }


  def saveSnapShotsToDisk(dir: String = "", tc: Long, alpha: Int = 2, l: Int = 2) : Unit = {
    var write = false
    var delete = false
    var order = 0
    val mcs = this.getMicroClusters
//    println("===========================================================OnlineResult-begin============================================================")
//    for (mc:MicroCluster <- mcs){
//      print(mc)
//    }
//    println("===========================================================OnlineResult-end=================================================================")
    val exp = (scala.math.log(tc) / scala.math.log(alpha)).toInt

    for (i <- 0 to exp) {
      if (tc % scala.math.pow(alpha, i + 1) != 0 && tc % scala.math.pow(alpha, i) == 0) {
        order = i
        write = true
      }
    }

    val tcBye = tc - ((scala.math.pow(alpha, l) + 1) * scala.math.pow(alpha, order + 1)).toInt

    if (tcBye > 0) delete = true

    if (write) {
      val out = new ObjectOutputStream(new FileOutputStream(dir + "/" + tc))

      try {
        out.writeObject(mcs)
        println("writted")
      }
      catch {
        case ex: IOException => println("Exception while writing file " + ex)
      }
      finally {
        out.close()
      }
    }

    if (delete) {
      try {
        new File(dir + "/" + tcBye).delete()
      }
      catch {
        case ex: IOException => println("Exception while deleting file " + ex);
      }
    }

  }



  /**
    *
    * @define 测试用
    * @return
    */
  override def toString = {
    "CluID:" +  Cluid
  }
}
protected class kmeansModel(var cf1x:Vector[Double],var n : Long)extends Serializable{
  var center: Vector[Double] = cf1x :/ n.toDouble
  def setCf1x(cf1x:Vector[Double]):Unit = {
    this.cf1x = cf1x
  }

  def getCf1x: Vector[Double] = {
    this.cf1x
  }
  def getCenter: Vector[Double] = {
    this.center
  }

  def setCenter(center:Vector[Double]): Unit = {
    this.center = center
  }

  def setN(n: Long): Unit = {
    this.n = n
  }

  def getN: Long = {
    this.n
  }
}




