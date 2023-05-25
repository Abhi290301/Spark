package Map_reduce_zip_z_parrallel

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeEx {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    var rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5))
    println("elements of rdd1")
    rdd1.foreach(x => print(x + ","))
    println()
    var rdd2 = sc.parallelize(List(6, 7, 8, 9, 10))
    println("elements of rdd2")
    rdd2.foreach(x => print(x +","))
    println()
    var rdd3 = rdd1.union(rdd2)
    println("elements of rdd3")
    rdd3.foreach(x => print(x + ","))
  }
}

object ParallelizeRDD{
  def main(args: Array[String]): Unit = {

  }
}