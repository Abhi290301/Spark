package Review2

import io.netty.handler.logging.LogLevel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object BroadCastJoins {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Broad Cast Joins")
      .getOrCreate()
    val sc = spark.sparkContext
    val size =104857600
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", size)
    println("Size set to " +size/(1024*1024) + "MB")
    sc.setLogLevel("OFF")
    println("spark.sparkContext.version")
    val rdd = spark.read.option("header", "true").option("delimiter", "true").option("inferSchema", "true").option("samplingRatio", 12)
      .parquet("C:\\tmp\\output\\Joins.parquet")
    val df = rdd.toDF()
    df.printSchema()
    df.show(truncate = false)

    //Creating a smaller Dataframe
    import spark.implicits._
    val rdd2 = Seq(("HP", "Himachal Pradesh"), ("PUN", "PUNJAB"), ("HR", "HARYANA"), ("Uk", "Utrakhand"))
    val smallerDF = rdd2.toDF("CODE", "STATE FULL NAME")
    smallerDF.show(false)

    df.join(
      broadcast(smallerDF),
      smallerDF("CODE") === df("State")
    ).show(false)

    df.join(
      broadcast(smallerDF),
      smallerDF("CODE") === df("State")
    ).explain(false)

    smallerDF.join(
      (df),
      df("State") === smallerDF("CODE")
    ).explain(false)

    while (true) {
      Thread.sleep(6000)
    }
  }

}

object BroadcastJoinExample {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("BroadcastJoinExample")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    // Enable broadcasting for small tables
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "52428800") // 50 MB

    // Read the large table
    val largeTableDF = spark.read
      .format("csv")
      .option("header", "true")
      .load("C:\\tmp\\projects_smaller.csv")

    // Read the small table
    val smallTableDF = spark.read
      .format("csv")
      .option("header", "true")
      .load("C:\\tmp\\stream.csv")

    // Perform the broadcast join
    val joinedDF = largeTableDF.join(broadcast(smallTableDF), "joinColumn")

    // Do further processing or output the result
    joinedDF.show()

    // Stop the SparkSession
    spark.stop()
  }
}
