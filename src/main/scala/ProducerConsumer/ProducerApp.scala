package ProducerConsumer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object ProducerApp {
  def main(args: Array[String]): Unit = {
    val topic = "json-data"
    val bootstrapServers = "localhost:9092"


    // Kafka producer properties
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // JSON records
    val jsonRecords = List(
      """{ 1, "John"}""",
      """{ 2, "Jane"}""",
      """{ 3, "Alice"}""",
      """{ 4, "Alice"}""",
      """{ 5, "Alice"}"""
    )

    try {
      jsonRecords.foreach { record =>
        val producerRecord = new ProducerRecord[String, String](topic, record)
        producer.send(producerRecord)
      }
    } finally {
      producer.close()
    }
  }


}
