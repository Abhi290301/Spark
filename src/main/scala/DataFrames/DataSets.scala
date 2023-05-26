package DataFrames

import org.apache.spark.sql.{SparkSession, Dataset}
object DataSets{
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.{SparkSession, Dataset}

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Spark Dataset Example")
      .getOrCreate()

    // Import implicit conversions for converting RDDs to Datasets
    import spark.implicits._

    // Create a case class representing your data structure
    case class Person(name: String, age: Int, city: String)

    // Create a sequence of data
    val data = Seq(
      Person("Alice", 25, "New York"),
      Person("Bob", 30, "San Francisco"),
      Person("Charlie", 35, "Chicago")
    )

    // Import the implicit Encoder for Person
    import spark.implicits._

//    // Create a Dataset
//    val dataset: Dataset[Person] = spark.createDataset(data)
//
//    // Perform operations on the Dataset
//    dataset.show()


  }
}