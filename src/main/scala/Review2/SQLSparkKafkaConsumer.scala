package Review2

import org.apache.spark.sql.SparkSession

object SQLSparkKafkaConsumer {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Spark Kafka Dosti")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "Spark-Topic")
      .load()

    df.printSchema()

    val df2 = df.selectExpr("CAST(key AS STRING)",
      "CAST(value AS STRING)", "topic")
    df2.show(100,truncate = false)
  }
}
