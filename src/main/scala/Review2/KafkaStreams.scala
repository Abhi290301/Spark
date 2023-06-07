package Review2

import org.apache.spark.sql.SparkSession

object KafkaStreams {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Kafka Streams")
      .getOrCreate()
spark.sparkContext.setLogLevel("OFF")
    //Read Data from the Topic
    spark.conf.set("spark.sql.streaming.checkpointLocation","C:\\tmp\\checkpoints")
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","Spark-Topic")
      .load()

    //perform transformation and analysis of data

    val processedData = df.selectExpr("CAST(key as String)","CAST(value as String)")

    //Start the streaming query

    val data = processedData.writeStream
      .outputMode("append")
      .format("json")
      .option("path","C:\\tmp\\output\\jsonfile")
      .start()
    data.awaitTermination()
  }

}
