package DataFrames

import org.apache.spark.sql.SparkSession

object DataFrameExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Data Frames")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val data = (Seq("Abhishek", "Abhinav", "Ajay"))

    val rdd = spark.sparkContext.parallelize(data)

    //Empty Dataframe
    val dfFromRDD = rdd.toDF()
    dfFromRDD.printSchema()
    //Parameterized
    val dfFromRDD1 = rdd.toDF(("Language"))
    dfFromRDD1.printSchema()

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
    val schema = StructType(Array(
      StructField("language", StringType, true),
      StructField("users", StringType, true),
      StructField("ID", IntegerType, true)
    ))
    val rowRDD = rdd.map(attributes => Row(attributes, attributes))
    val dfFromRDD3 = spark.createDataFrame(rowRDD, schema)
    dfFromRDD3.printSchema()

    //From Data (USING createDataFrame and Adding schema using StructType)
    val rowData = Seq(Row("Java", "20000"),
      Row("Python", "100000"),
      Row("Scala", "3000"))
    val dfFromRDD4 = spark.createDataFrame(rowRDD, schema)
    dfFromRDD4.printSchema()
  }

}
