package Kafka8623

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.io.File
import java.util.Properties

object AvroProduce {
  def main(args: Array[String]): Unit = {

    try {
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      props.put("value.converter.schema.registry.url", "http://localhost:8081")
      props.put("key.converter.schema.registry.url", "http://localhost:8081")
      props.put("schema.registry.url", "http://localhost:8081")
      props.put("avro.serializer.permissive", "true")

      val producer = new KafkaProducer[String, GenericRecord](props)

      val avropath = "C:\\tmp\\output\\twitter.avro"

      val datumReadr = new GenericDatumReader[GenericRecord]()

      val dataFileReader = new DataFileReader[GenericRecord](new File(avropath), datumReadr)
      // Get the Avro schema from the file's metadata
      val avroSchema: Schema = dataFileReader.getSchema
      println(avroSchema.toString)
      while (dataFileReader.hasNext) {
        val avroReader = dataFileReader.next()
        val record = new ProducerRecord[String, GenericRecord]("Testing-Topic", avroReader)
        producer.send(record)
      }
      dataFileReader.close()
      producer.close()
    } catch {
      case e : Exception =>println(e.printStackTrace())
    }
  }
}
