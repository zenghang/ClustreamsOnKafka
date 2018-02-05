package org.kafkastreams.clustream

import java.io.Serializable
import java.util.Properties

import org.apache.commons.math3.stat.inference.TestUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}

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
    val Clu = new CluStreamOnline(20,3,10)

    val inputTopic = "CluStream-inputTopic"
    val bootstrapServers = "localhost:9092"
    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "CluStreamOnKStream-test")
      p.put(StreamsConfig.CLIENT_ID_CONFIG, "CluStreamOnKStream-test-client")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      // The commit interval for flushing records to state stores and downstream must be lower than
      // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
      p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
      p
    }

    val stringSerde: Serde[String] = Serdes.String()

    val builder: StreamsBuilder = new StreamsBuilder()

    val inputData : KStream[String,String] = builder.stream(inputTopic)

    val streams : KafkaStreams = new KafkaStreams(builder.build(),streamsConfiguration)

    streams.cleanUp()
    streams.start()

    Clu.run(inputData)


  }
}

