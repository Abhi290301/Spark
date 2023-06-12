package Main_Programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object ConsumerWindowing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaWindowing")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val topic = "json-data"

    val schema = StructType(
      List(
        StructField("id", IntegerType),
        StructField("name", StringType)
      )
    )

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    val jsonDF = df.selectExpr("CAST(value AS STRING) AS json")

    val jsonData = jsonDF.select(from_json(col("json"), schema).as("data"))

    val extractedDF = jsonData.select("data.*")
      .withColumn("timestamp", current_timestamp())

    val windowedDF = extractedDF
      .groupBy(window(col("timestamp"), "5 seconds"))
      .agg(collect_list(col("id")).as("ids"), collect_list(col("name")).as("names"))

    val query = windowedDF.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}



object ConsumerWindowingHopping {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaWindowing")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val topic = "json-data"

    val schema = StructType(
      List(
        StructField("id", IntegerType),
        StructField("name", StringType)
      )
    )

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    val jsonDF = df.selectExpr("CAST(value AS STRING) AS json")

    val jsonData = jsonDF.select(from_json(col("json"), schema).as("data"))

    val extractedDF = jsonData.select("data.*")
      .withColumn("timestamp", current_timestamp())

    val windowedDF = extractedDF
      .groupBy(window(col("timestamp"), "5 seconds", "2 seconds")) // 5-second window with 2-second sliding interval
      .agg(collect_list(col("id")).as("ids"), collect_list(col("name")).as("names"))

    val query = windowedDF.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
