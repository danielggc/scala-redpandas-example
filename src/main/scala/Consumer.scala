import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.StreamsBuilder

import java.util.{Collections, Properties}

object Consumer {
    import scala.collection.JavaConverters._

    def main(args: Array[String]): Unit = {

        println("init consumer ")
        val props = new Properties()
        props.put("bootstrap.servers", "localhost:9092")
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("group.id", "something")

        val TOPIC = "messages"


        val consumer = new KafkaConsumer(props)
        consumer.subscribe(Collections.singletonList(TOPIC))

        while (true) {
            val records = consumer.poll(30)
            for (record <- records.asScala) {
                println(record)
            }
        }
        consumer.close()
    }

}
