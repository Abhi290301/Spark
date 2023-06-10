import org.apache.commons.collections.CollectionUtils.select

import java.nio.file.{Files, Paths}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.col

object AvroConsumer1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local").appName("Consumer Avro")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","Testing-Topic")
      .option("startingOffsets","latest").load()

    df.printSchema()

    /*
        Read schema to convert Avro data to DataFrame
         */    val jsonFormatSchema = new String(
      Files.readAllBytes(Paths.get("C:\\tmp\\output\\person.avro")))

      val personDf = df.select(from_avro(col("value"),jsonFormatSchema).as( "person"))
    .select("person.*")

    personDf.printSchema()

    personDf.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

}
