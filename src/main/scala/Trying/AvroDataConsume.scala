package Trying

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.col

object AvroDataConsume {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Trying to run")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")  // Corrected option name
      .option("subscribe", "AvroTopic")
      .option("startingOffset", "earliest")
      .load()

    val avroSchema =
      """{"type":"record",
        |"name":"example_schema",
        |"namespace":"com.example.avro",
        |"fields":[
        |{"name":"id","type":"int"},
        |{"name":"name","type":"string"},
        |{"name":"age","type":"int"}]}""".stripMargin

    val personDf = df.select(from_avro(col("value"), avroSchema).as("person"))

    val extractedDF = personDf.select("person.*")

    val query = extractedDF.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
