package Kafka9623

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{Consumed, KStream}
import org.apache.kafka.streams.scala.{StreamsBuilder, kstream}

import java.util.Properties

object KafkaStream {
  def main(args: Array[String]): Unit = {
    val topic = "json-data"

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG,"Kafka-Stream-Connect")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    props.put("auto.offset.reset","earliest")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass)

    val builder = new StreamsBuilder()

    // Create a KStream from the input topic
    val stream: kstream.KStream[String, String] = builder.stream[String, String](topic)(Consumed.`with`(Serdes.String(), Serdes.String()))

    stream.foreach((key,value)=>println(s"Key : $key , Value : $value"))

    val streams = new KafkaStreams(builder.build(),props)

    streams.start()

    Runtime.getRuntime.addShutdownHook(new Thread(
      ()=>{
        streams.close()
      }
    ))
  }
}
