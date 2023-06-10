import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.jdk.CollectionConverters.IterableHasAsScala

object WindowingConsumer {
  def main(args: Array[String]): Unit = {
    val topic = "AvroTopic"

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "your-consumer-group")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
    props.put("schema.registry.url", "http://localhost:8081")

    val consumer = new KafkaConsumer[String, GenericRecord](props)
    consumer.subscribe(java.util.Collections.singletonList(topic))

    try {
      while (true) {
        val records = consumer.poll(Duration.ofMillis(100))
        for (record <- records.asScala) {
          val value = record.value()
          println(s"Value: $value")
          // Process the Avro record as needed
        }
      }
    } finally {
      consumer.close()
    }
  }
}
