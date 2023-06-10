import java.util.Properties
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object WindowingProducer {
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

    // Create a scanner to read input from the console
    val scanner = new java.util.Scanner(System.in, "UTF-8")

    try {
      while (true) {
        // Prompt for input
        print("Enter data (id,name,age): ")
        val input = scanner.nextLine()

        // Split the input and extract id, name, and age
        val Array(id, name, age) = input.split(",").map(_.trim)

        // Create a new Avro record
        val record = new GenericData.Record(new Schema.Parser().parse(avroSchema))
        record.put("id", id.toInt)
        record.put("name", name)
        record.put("age", age.toInt)

        // Send the Avro record to the Kafka topic
        val key = id
        val value = record
        val producerRecord = new ProducerRecord[String, GenericRecord]("AvroTopic", key, value)
        producer.send(producerRecord)

        // Sleep for 1 second between sending records
        Thread.sleep(1000)
      }
    } finally {
      producer.close()
    }
  }
}
