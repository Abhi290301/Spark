package PracticeKafka


import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.jdk.CollectionConverters.IterableHasAsScala

object AvroConsumer {
  def main(args: Array[String]): Unit = {
    val topic = "Testing-Topic"

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "your-consumer-group")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
    props.put("schema.registry.url", "http://localhost:8081")
    props.put("specific.avro.reader", "true") // Set to true if using SpecificRecord Avro classes

    val consumer = new KafkaConsumer[String, GenericRecord](props)
    consumer.subscribe(Collections.singletonList(topic))

    try {
      while (true) {
        val records = consumer.poll(java.time.Duration.ofMillis(100))
        for (record: ConsumerRecord[String, GenericRecord] <- records.asScala) {

          val value = record.value()

          println(s" Value: $value")
          // Process the Avro record as needed
        }
      }
    } finally {
      consumer.close()
    }
  }
}
