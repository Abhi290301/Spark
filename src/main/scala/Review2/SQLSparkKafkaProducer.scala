package Review2

import org.apache.spark.sql.SparkSession

object SQLSparkKafkaProducer {
  def main(args: Array[String]): Unit = {


      val spark = SparkSession.builder()
        .master("local[*]")
        .appName("Spark Kafka Dost")
        .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
      val data = Seq(("iphone", "2007"), ("iphone 3G", "2008"),
        ("iphone 3GS", "2009"),
        ("iphone 4", "2010"),
        ("iphone 4S", "2011"),
        ("iphone 5", "2012"),
        ("iphone 8", "2014"),
        ("iphone 10", "2017"))

      val df = spark.createDataFrame(data).toDF("key", "value")
      /*
        since we are using dataframe which is already in text,
        selectExpr is optional.
        If the bytes of the Kafka records represent UTF8 strings,
        we can simply use a cast to convert the binary data
        into the correct type.
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      */
      df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "Spark-Topic")
        .save("C:\\tmp\\output\\spark-kafka")

  }


}
