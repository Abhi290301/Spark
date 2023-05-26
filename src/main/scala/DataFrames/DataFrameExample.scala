package DataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

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
    dfFromRDD1.show()

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


//Other Example


object NewDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Testing New")
      .getOrCreate()
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(Seq(("Abhishek", 1), ("Aditi", 2), ("Akash", 3), ("Anchal", 4)))
    val columns = Seq("Names", "ID")
    val dfFromRDD = rdd.toDF()
    dfFromRDD.printSchema()

    val dffFromRDD1 = rdd.toDF("Name", "ID")
    dffFromRDD1.printSchema()

    //createDataFrames
    val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns: _*)
    dfFromRDD2.printSchema()

    //Adding a column
    val newdf = dffFromRDD1.withColumn("User", lit("1"))
    println("New Df :" + newdf.printSchema())
    newdf.show()
    //Row Type
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._
    val schema = StructType(Array
    (
      StructField("Name", StringType, nullable = true)
      , StructField("ID", IntegerType, nullable = true)
    ))
    val rowRDD = rdd.map(attribute => Row(attribute._1, attribute._2))
    val rdd4 = spark.createDataFrame(rowRDD, schema)
    rdd4.printSchema()
    rdd4.show()


    //Creating a table

    val arrayStrct = Seq(
      Row(Row("Abhishek", "", "Chandel"), List("Java", "Android", "Scala"), "Himachal", "Male", List(10, 20),30),
      Row(Row("Akash", "", ""), List("Data Science"), "Delhi", "Male", List(10, 20),200),
      Row(Row("Nikhil", "kumar", "Rana"), List("C++", "Angular"), "Kangra", "Male", List(10, 20),20)
    )

    val structScheme = new StructType()
      .add("name", new StructType()
        .add("firstName", StringType)
        .add("middleName", StringType)
        .add("lastName", StringType)
      )
      .add("languages", ArrayType(StringType))
      .add("state", StringType)
      .add("Gender", StringType)
      .add("remarks", ArrayType(IntegerType))
      .add("salary",IntegerType)


    val cf = spark.createDataFrame(spark.sparkContext.parallelize(arrayStrct), structScheme)
    cf.printSchema()
    cf.show()
    val cf2 = cf.withColumn("Country",lit("India")).withColumn("hike",col("salary")*100)
    cf2.show(false)


    cf2.show(false)

    println("Now using the Where "+cf2.where(cf2("hike")===2000).show(false))



  }
}
