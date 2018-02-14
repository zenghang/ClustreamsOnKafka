package org.kafkastreams.clustream

import java.io.Serializable
import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.{Callable, Executors, TimeUnit}

import breeze.linalg._
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{Consumed, KafkaStreams, StreamsBuilder, StreamsConfig}
import org.kafkastreams.clustream.mcInfo.McInfo

import scala.util.control.Breaks

/**
  * Created by 11245 on 2018/1/23.
  */
class Clustream (
                  val model:CluStreamOnline)
  extends Serializable{

  def this() = this(null)


}

object Clustream{
  def main(args: Array[String]): Unit = {


    val numThreads = 1

    val threadPool = Executors.newFixedThreadPool(numThreads)
    val threadPool2 = Executors.newFixedThreadPool(1)
    for (i <- 0 until numThreads){
      threadPool.submit(new OnlineTask)
    }
    threadPool2.submit(new GloableOnlineTask)

  }
}


class OnlineTask extends Runnable{
  override def run(): Unit = {
    val inputTopic = "CluStreamTest1"
    val middleTopic = "CluSterAsPointTest0"

    val schemaRegistryUrl = "http://localhost:8081"
    val bootstrapServers = "localhost:9092"
//    val bootstrapServers = "192.168.222.226:9092"

    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "CluStreamOnKStream-test0")
      p.put(StreamsConfig.CLIENT_ID_CONFIG, "CluStreamOnKStream-test-client")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
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
    inputData.foreach{ (k,v) =>
      val data : Vector[Double]= Vector(v.split(",").map(_.toDouble))
      println("=========================================================================Online print==================================================================================")
      println(Clu)
      Clu.run(data)
    }

    val timerPool = Executors.newScheduledThreadPool(1)
    timerPool.scheduleAtFixedRate(new SendTask(Clu,produerProperties,schemaRegistryUrl,middleTopic),20,20,TimeUnit.SECONDS)

    val streams : KafkaStreams = new KafkaStreams(builder.build(),streamsConfiguration)

    streams.cleanUp()
    streams.start()






  }
}
class GloableOnlineTask extends Runnable {
  override def run(): Unit = {
    val inputTopic = "CluSterAsPointTest0"
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
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[SpecificAvroSerde[_ <: SpecificRecord]])
      // The commit interval for flushing records to state stores and downstream must be lower than
      // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
      p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
      p
    }

    val serdeConfig: util.Map[String, String] = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
    val McInfoSerde = new SpecificAvroSerde[McInfo]
    McInfoSerde.configure(serdeConfig, false)
    val builder: StreamsBuilder = new StreamsBuilder()

    val inputData: KStream[String, McInfo] = builder.stream(inputTopic, Consumed.`with`(Serdes.String, McInfoSerde))
    //测试是否能取到数据
//    val data : KStream[String, McInfo] = inputData.peek{(k,v) =>
//      println("key is :" + k)
//      println("N is :" + v.getN)
//      println("Time is :" + v.getCf1t)
//    }
    val Clu = new CluStreamOnline(10, 3, 10)
    inputData.foreach { (k, v) =>
      val data: Vector[Double] = Vector(k.split(",").map(_.toDouble))

      println("-------------------------------------------------------------------------Gloable print----------------------------------------------------------------------------------")
      println(Clu)
      Clu.globalrun(data, v)
    }
    val streams: KafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration)

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
        if(mc.n != 0)
          has = true
      })
      has
    }

    if(hasAnypoint){
      println("*************************************************************************Send to Middle Topic**********************************************************************************")
      println("before Time:"+cluOnline.getCurrentTime)
      cluOnline.sendClustersToTopic(topicName,schemaRegistryUrl,producerProperties)
      cluOnline.initRandom
      println("after Time:"+cluOnline.getCurrentTime)
      println("*************************************************************************Send to Middle Topic END**********************************************************************************")
    }
  }
}