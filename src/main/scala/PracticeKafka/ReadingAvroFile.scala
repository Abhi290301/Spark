package PracticeKafka

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificDatumReader
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.io.File
import java.util.Properties

object ReadingAvroFile {
  def main(args: Array[String]): Unit = {
    val avroFile = new File("C:\\tmp\\output\\twitter.avro")
    val topic = "Testing-Topic"

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put("schema.registry.url", "http://localhost:8081")

    val producer = new KafkaProducer[String, GenericRecord](props)

    val reader = new SpecificDatumReader[GenericRecord]()
    val dataFileReader = new DataFileReader[GenericRecord](avroFile, reader)

    try {
      while (dataFileReader.hasNext) {
        val record = dataFileReader.next()
      // Replace "keyField" with the appropriate key field name
        val valueField = record.get("value") // Replace "valueField" with the appropriate value field name

        record.getSchema
        val producerRecord = new ProducerRecord[String, GenericRecord](topic,valueField.asInstanceOf[GenericRecord])
        producer.send(producerRecord)
      }
    } finally {
      dataFileReader.close()
      producer.close()
    }
  }
}
