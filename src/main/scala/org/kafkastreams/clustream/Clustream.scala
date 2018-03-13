package org.kafkastreams.clustream

import java.io.{FileInputStream, IOException, ObjectInputStream, Serializable}
import java.nio.file.{Files, Paths}
import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.{Executors, TimeUnit}
import breeze.linalg._
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization. Serdes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{Consumed, KafkaStreams, StreamsBuilder, StreamsConfig}
import org.kafkastreams.clustream.mcInfo.McInfo
import scala.util.control.Breaks

/**
  * Created by 11245 on 2018/1/23.
  */
class Clustream (
                  val model:CluStreamOnline = new CluStreamOnline(10,3,10))
  extends Serializable{

  val schemaRegistryUrl = "http://localhost:8081"
  val bootstrapServers = "localhost:9092"
  val applicationServerPort = "localhost:8080"

  val streamsConfiguration: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "CluStreamOnKStream-test1")
    p.put(StreamsConfig.CLIENT_ID_CONFIG, "CluStreamOnKStream-test1-client")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
    p.put(StreamsConfig.APPLICATION_SERVER_CONFIG, applicationServerPort)
    //p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[SpecificAvroSerde[_ <: SpecificRecord]])
    // The commit interval for flushing records to state stores and downstream must be lower than
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
    p
  }

  def startOnline() : Unit = {
    val inputTopic = "CluSterAsPointTest1"

    val serdeConfig: util.Map[String, String] = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
    val McInfoSerde = new SpecificAvroSerde[McInfo]
    McInfoSerde.configure(serdeConfig, false)
    val builder: StreamsBuilder = new StreamsBuilder()

    val inputData: KStream[String, McInfo] = builder.stream(inputTopic, Consumed.`with`(Serdes.String, McInfoSerde))

    inputData.foreach { (k, v) =>

      val data: Vector[Double] = Vector(k.split(",").map(_.toDouble))

      println("-------------------------------------------------------------------------Global print----------------------------------------------------------------------------------")
      println(v.getCf1x)
      model.globalrun(data, v)
    }

    val numThread = 1
    val threadPool = Executors.newFixedThreadPool(numThread)
    for(i<-1 to numThread)
      threadPool.submit(new OnlineTask)
    val timePool = Executors.newScheduledThreadPool(1)
    timePool.scheduleAtFixedRate(new ClockAndSaveTask(model),22,20,TimeUnit.SECONDS)

    val streams: KafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration)

    streams.cleanUp()
    streams.start()
  }


  def getSnapShots(dir: String = "", tc: Long, h: Long): (Long,Long) = {

    var tcReal = tc
    while(!Files.exists(Paths.get(dir + "/" + tcReal)) && tcReal >= 0) tcReal = tcReal - 1
    var tcH = tcReal - h
    while(!Files.exists(Paths.get(dir + "/" + tcH)) && tcH >= 0) tcH = tcH - 1
    if(tcH < 0) while(!Files.exists(Paths.get(dir + "/" + tcH))) tcH = tcH + 1

    if(tcReal == -1L) tcH = -1L
    (tcReal, tcH)
  }

  def getMCsFromSnapshots(dir: String = "", tc: Long, h: Long): Array[MicroCluster] = {
    val (t1,t2) = getSnapShots(dir,tc,h)

    try{
      val in1 = new ObjectInputStream(new FileInputStream(dir + "/" + t1))
      val snap1 = in1.readObject().asInstanceOf[Array[MicroCluster]]

      val in2 = new ObjectInputStream(new FileInputStream(dir + "/" + t2))
      val snap2 = in2.readObject().asInstanceOf[Array[MicroCluster]]

      in2.close()
      in1.close()

      val arrs1 = snap1.map(_.getIds)
      val arrs2 = snap2.map(_.getIds)

      val relatingMCs = snap1 zip arrs1.map(a => arrs2.zipWithIndex.map(b=> if(b._1.toSet.intersect(a.toSet).nonEmpty) b._2;else -1))
      relatingMCs.map{ mc =>
        if (!mc._2.forall(_ == -1) && t1 - h  >= t2) {
          for(id <- mc._2) if(id != -1) {
            mc._1.setCf2x(mc._1.getCf2x :- snap2(id).getCf2x)
            mc._1.setCf1x(mc._1.getCf1x :- snap2(id).getCf1x)
            mc._1.setCf2t(mc._1.getCf2t - snap2(id).getCf2t)
            mc._1.setCf1t(mc._1.getCf1t - snap2(id).getCf1t)
            mc._1.setN(mc._1.getN - snap2(id).getN)
            mc._1.setIds(mc._1.getIds.toSet.diff(snap2(id).getIds.toSet).toArray)
          }
          mc._1
        }else mc._1

      }
    }
    catch{
      case ex: IOException => println("Exception while reading files " + ex)
        null
    }

  }

  private def sample[A](dist: Map[A, Double]): A = {
    val p = scala.util.Random.nextDouble
    val it = dist.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb
      if (accum >= p)
        return item
    }
    sys.error(f"this should never happen") // needed so it will compile
  }
  def getCentersFromMC(mcs: Array[MicroCluster]): Array[Vector[Double]] = {
    mcs.filter(_.getN > 0).map(mc => mc.getCf1x :/ mc.getN.toDouble)
  }

  def getWeightsFromMC(mcs: Array[MicroCluster]): Array[Double] = {
    val arr: Array[Double] = mcs.map(_.getN.toDouble).filter(_ > 0)
    val sum: Double = arr.sum
    arr.map(value => value/sum)
  }


  def fakeKMeans(k:Int,numPoints:Int,mcs: Array[MicroCluster],numDimensions:Int): Array[kmeansModel] ={
    val Clusters : Array[kmeansModel]= Array.fill(k)(new kmeansModel(Vector.fill[Double](numDimensions)(0.0),0))
    val centers = getCentersFromMC(mcs)

    for (i <- 0 until k){
      Clusters(i).setCenter(centers(i))
    }
    val weights = getWeightsFromMC(mcs)
    val map = (centers zip weights).toMap
    val points = Array.fill(numPoints)(sample(map))
    var repetitions = 10
    while (repetitions >=0){
      for (point <- points){
        //Assign points to Clusters
        var minDistance = squaredDistance(point,Clusters(0).getCenter)
        var closestCluster = 0
        var len = mcs.length
        for (i <- 1 until Clusters.length){
          val distance = squaredDistance(point,Clusters(i).getCenter)
          if (distance < minDistance){
            closestCluster = i
            minDistance = distance
          }
        }
        //将点加进去
        Clusters(closestCluster).setCf1x(Clusters(closestCluster).getCf1x:+point)
        Clusters(closestCluster).n += 1
      }
      //Calculate new centers
      for (i <- 0 until  Clusters.length){
        Clusters(i).setCenter(Clusters(i).getCf1x / Clusters(i).n.toDouble)
      }
      repetitions -= 1
    }
    Clusters
  }

  def sumVector(vector: Vector[Double]):Double = {
    var sum = 0.0
    val dArr =  vector.toArray
    for (cf2x <- dArr){
      sum = sum + cf2x
    }
    sum
  }


}

object Clustream{
  def main(args: Array[String]): Unit = {
    val inputTopic = "CluStreamTest2"
    //val inputTopic = "dataset"

    val schemaRegistryUrl = "http://localhost:8081"
    val bootstrapServers = "localhost:9092"
    //    val bootstrapServers = "192.168.222.226:9092"

    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "CluStreamOnKStream-test0")
      p.put(StreamsConfig.CLIENT_ID_CONFIG, "CluStreamOnKStream-test-client")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      //p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      // The commit interval for flushing records to state stores and downstream must be lower than
      // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
      p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
      p
    }

    val produerProperties: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      p
    }

    val builder: StreamsBuilder = new StreamsBuilder()

    val inputData : KStream[String,String] = builder.stream(inputTopic)

    val Clu = new CluStreamOnline(10,3,10)

    inputData.foreach{ (k,v) =>
      val data : Vector[Double]= Vector(v.split(",").map(_.toDouble))
//      println("=========================================================================Online print==================================================================================")
//      println(data)
      Clu.run(data)
    }

    val timerPool = Executors.newScheduledThreadPool(2)
    timerPool.scheduleAtFixedRate(new ClockTask(Clu),1,1,TimeUnit.SECONDS)
    timerPool.scheduleAtFixedRate(new SaveTask(Clu),1,1,TimeUnit.SECONDS)

    val streams : KafkaStreams = new KafkaStreams(builder.build(),streamsConfiguration)

    streams.cleanUp()
    streams.start()

  }
}


class OnlineTask extends Runnable{
  override def run(): Unit = {
    val inputTopic = "CluStreamTest1"
    //val inputTopic = "dataset"
    val middleTopic = "CluSterAsPointTest1"

    val schemaRegistryUrl = "http://localhost:8081"
    val bootstrapServers = "localhost:9092"
//    val bootstrapServers = "192.168.222.226:9092"

    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "CluStreamOnKStream-test0")
      p.put(StreamsConfig.CLIENT_ID_CONFIG, "CluStreamOnKStream-test-client")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      // The commit interval for flushing records to state stores and downstream must be lower than
      // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
      p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
      p
    }

    val produerProperties: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      p
    }

    val builder: StreamsBuilder = new StreamsBuilder()

    val inputData : KStream[String,String] = builder.stream(inputTopic)

    val Clu = new CluStreamOnline(20,3,10)
//    println(Clu.toString);
    //kmeans初始化放到这里
    //    val Indata : KStream[String, McInfo] = inputData.peek{(k,v) =>
    //      val data: Vector[Double] = Vector(k.split(",").map(_.toDouble))
    //      Clu.initKmeans(data)
    //    }
    inputData.foreach{ (k,v) =>
      val data : Vector[Double]= Vector(v.split(",").map(_.toDouble))
      Clu.run(data)
    }

    val timerPool = Executors.newScheduledThreadPool(2)
    timerPool.scheduleAtFixedRate(new SendTask(Clu,produerProperties,schemaRegistryUrl,middleTopic),20,20,TimeUnit.SECONDS)
    timerPool.scheduleAtFixedRate(new ClockTask(Clu),1,1,TimeUnit.SECONDS)

    val streams : KafkaStreams = new KafkaStreams(builder.build(),streamsConfiguration)

    streams.cleanUp()
    streams.start()

  }
}


class SendTask (val cluOnline : CluStreamOnline,val producerProperties: Properties,val schemaRegistryUrl:String,val topicName:String ) extends Runnable {
  override def run(): Unit = {

    val hasAnypoint : Boolean = {
      val loop  = new Breaks
      var has = false

      loop.breakable(for (mc <- cluOnline.getMicroClusters){
        if(mc.n != 0){
          has = true
          loop.break();
        }
      })
      has
    }

    if(hasAnypoint){
      println("*************************************************************************Send to Middle Topic**********************************************************************************")
      println("before Time:"+cluOnline.getCurrentTime)
      cluOnline.sendClustersToTopic(topicName,schemaRegistryUrl,producerProperties)
      println("after Time:"+cluOnline.getCurrentTime)
      println("*************************************************************************Send to Middle Topic END**********************************************************************************")
    }
  }
}

class ClockAndSaveTask(globalOnlineClu: CluStreamOnline) extends Runnable {
  override def run(): Unit = {
    val time = globalOnlineClu.getAtomicGlobalTime.incrementAndGet()
    globalOnlineClu.saveSnapShotsToDisk("/Users/hu/KStream/snaps",time,2,4)
  }
}


class ClockTask(onlineClu: CluStreamOnline) extends Runnable {
  override def run() = onlineClu.getAtomicTime.incrementAndGet()
}

//用来对比的
class SaveTask(onlineClu: CluStreamOnline) extends Runnable {
  override def run(): Unit = {
    val time = onlineClu.getCurrentTime
    onlineClu.saveSnapShotsToDisk("/Users/hu/KStream/snaps2",time,2,4)
  }
}