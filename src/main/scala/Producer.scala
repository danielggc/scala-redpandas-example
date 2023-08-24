import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.StreamsBuilder

import java.util.Properties




object  Producer  {

  def main ( args :Array[String] ): Unit = {
    println(" init producer ")
    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val TOPIC = "messages"
    println(" configs ")
    val builder = new StreamsBuilder

    for (i <- 1 to 50) {
      val record = new ProducerRecord(TOPIC, "key", s"hello $i")
      val dd  = producer.send(record)
      println(s"result =====> ${dd}")

    }

    producer.close()

  }
}