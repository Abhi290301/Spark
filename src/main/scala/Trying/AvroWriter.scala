import io.confluent.kafka.serializers.KafkaAvroSerializer

import java.util.Properties
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object AvroWriter {
  def main(args: Array[String]): Unit = {
    // Define Avro schema
    val avroSchema =
      """{"type":"record",
        |"name":"example_schema",
        |"namespace":"com.example.avro",
        |"fields":[
        |{"name":"id","type":"int"},
        |{"name":"name","type":"string"},
        |{"name":"age","type":"int"}]}""".stripMargin

    // Create Kafka producer properties
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.put("key.serializer", classOf[StringSerializer].getName)
    kafkaProps.put("value.serializer", classOf[KafkaAvroSerializer].getName)
    kafkaProps.put("schema.registry.url", "http://localhost:8081") // Set the URL of the Schema Registry

    // Create Kafka producer
    val producer = new KafkaProducer[String, GenericRecord](kafkaProps)

    // Continuously read input from console and send Avro records to Kafka topic
    val scanner = new java.util.Scanner(System.in, "UTF-8")

    while (true) {
      print("Enter data (id,name,age): ")
      val input = scanner.nextLine()

      val Array(id, name, age) = if (input.split(",", -1).length == 3) input.split(",", -1).map(_.trim) else Array("", "", "")

      val record = new GenericData.Record(new Schema.Parser().parse(avroSchema))
      record.put("id", id.toInt)
      record.put("name", name)
      record.put("age", age.toInt)

      val key = id
      val value = record
      val producerRecord = new ProducerRecord[String, GenericRecord]("AvroTopic", key, value)
      producer.send(producerRecord)
    }

    producer.close()
  }
}
