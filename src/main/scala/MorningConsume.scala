import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, window}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object MorningConsume {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Trying to run")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

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

    // Convert the JSON data to structured DataFrame using the provided schema
    val jsonData = jsonDF.select(from_json(jsonDF("json"), schema).as("data"))

    // Extract the fields from the structured DataFrame
    val extractedDF = jsonData.select("data.*")

    val query = extractedDF.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation","C:\\tmp\\checkpoints")
      .option("truncate", "false")// Disable truncation of column values
      .start()
    query.awaitTermination()
  }
}
