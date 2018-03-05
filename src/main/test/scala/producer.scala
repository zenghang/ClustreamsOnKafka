import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
object Produceratatotopic {
  def main(args: Array[String]): Unit = {
    val messagesPerSec = 3
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    var i :Int = 0;
    // Send some messages
    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = scala.util.Random.nextInt(50).toString +","+ scala.util.Random.nextInt(100).toString +","+ scala.util.Random.nextInt(50).toString
        println(str+"---"+i)
        i=i+1
        val message = new ProducerRecord[String, String]("CluStreamTest2", null, str)
        producer.send(message)
      }
      Thread.sleep(1000)
    }
  }

}
