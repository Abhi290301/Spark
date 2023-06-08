package Kafka8623

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.col

object AvroConsume {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Get Avro Record")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "Testing-Topic")
      .option("starting-offset", "earliest")
      .load()

    val avroSchema =
      """{"type":"record",
        |"name":"twitter_schema",
        |"namespace":"com.miguno.avro",
        |"fields":
        |[{"name":"username"
        |,"type":"string",
        |"doc":"Name of the user account on Twitter.com"},
        |{"name":"tweet","type":"string","doc":"The content of the user's Twitter message"},
        |{"name":"timestamp","type":"long","doc":"Unix epoch time in seconds"}],
        |"doc:":"A basic schema for storing Twitter messages"}
        |""".stripMargin

  val personDF = df.select(from_avro(col("value"),avroSchema).as("person"))
    val extractedDF = personDF.select("person.*")

    extractedDF.printSchema()

    val query = extractedDF.writeStream
      .format("console")
      .option("mode","PERMISSIVE")
      .start()

    query.awaitTermination()
  }

}
