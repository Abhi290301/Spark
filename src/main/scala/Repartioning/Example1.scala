package Repartioning

import org.apache.spark.sql.SparkSession

object Example1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Repartitioning")
      .master("local[*]")
      .getOrCreate()

    val rdd1 = spark.sparkContext.parallelize(Range(0,30))
    println("Partition from Local:"+rdd1.partitions.size)
    println("Partition from Local:"+rdd1.partitions)

    val rdd2 = spark.sparkContext.parallelize(Range(0, 30),6)
    println("Partition from Local:" + rdd2.partitions.size)
    println("Partition from Local:" + rdd2.partitions)
  }
}
