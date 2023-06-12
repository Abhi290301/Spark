//object ProtobufProducer {
//  def main(args: Array[String]): Unit = {
//    import java.util.Properties
//    import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//    import com.google.protobuf.ByteString
//
//    // Create a Kafka producer configuration
//    val props = new Properties()
//    props.put("bootstrap.servers", "localhost:9092")
//    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
//    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
//
//    // Create a Kafka producer
//    val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)

//    // Create your Protobuf message
//    val protobufMessage = YourProtobufMessage.newBuilder()
//      .setField1("value1")
//      .setField2(123)
//      .build()

//    // Serialize the Protobuf message to a byte array
//    val serializedMessage = protobufMessage.toByteArray

//    // Create a Kafka producer record with the serialized message
//    val record = new ProducerRecord[Array[Byte], Array[Byte]]("your_topic", serializedMessage)

//    // Send the record to Kafka
//    producer.send(record)


//    // Close the producer
//    producer.close()
//
//  }
//}
