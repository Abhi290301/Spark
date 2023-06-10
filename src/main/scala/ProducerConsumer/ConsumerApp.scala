package ProducerConsumer

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import java.util.Properties

object ConsumerApp {
  def main(args: Array[String]): Unit = {

    val topic = "json-data"
    val bootstrapServers = "localhost:9092"
    val groupId = "json-data-loading"

    // Kafka consumer properties
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    val consumer = new KafkaConsumer[String, String](props)

    try {
      consumer.subscribe(java.util.Collections.singletonList(topic))

      while (true) {
        val records = consumer.poll(java.time.Duration.ofMillis(100))
        val cdf = records.forEach { record =>
          val value = record.value()

          println(s"Received record: Group ID = $groupId value(id,name) = $value")

        }
      }
    } finally {
      consumer.close()
    }
  }
}
