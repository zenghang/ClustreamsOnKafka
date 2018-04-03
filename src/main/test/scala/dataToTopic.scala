import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.control.Breaks

object dataToTopic {
  def main(args: Array[String]): Unit = {
    val messagesPerSec = 500
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    val pointNum = 100000
    val readPath = "/home/hadoop/clustream/dataset/data.txt"
    val read: BufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(readPath)))
    var i = 0
    var num =1
    var point : String= ""
    val points = new Array[String](pointNum)
    val loop = new Breaks


    loop.breakable{
      while(num <= pointNum){
        for(i <- 1 to messagesPerSec){
          point = read.readLine()
          if(point!=null){
            val message = new ProducerRecord[String, String]("CluStreamTest2", null, point)
            val message1 = new ProducerRecord[String, String]("CluStreamTest1", null, point)
            val message2 = new ProducerRecord[String, String]("CluStreamTest3", null, point)
            //producer.send(message)
            //producer.send(message1)
            producer.send(message2)
            println(point+"---"+num)
            num+=1
          }
          else
            loop.break()
        }
        Thread.sleep(1000)
      }
    }



  }

}
