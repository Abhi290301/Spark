package main1

import org.apache.spark.{SparkConf, SparkContext}

object SparkSessionEx {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Test")


    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val rddCollect: Array[Int] = rdd.collect()
    println("Number of Partitions: " + rdd.getNumPartitions)
    println("Action: First element: " + rdd.first())
    println("Action: First element: " + rdd.cache())
    println("Action: RDD converted to Array[Int] : ")
    rddCollect.foreach(println)
  }

}

object SessionCreate extends App {


  var cong = new SparkConf()
    .setAppName("Local[*]")
    .setAppName("TestingSession")

  println("Class" + cong.getClass)

  println("App master" + cong.setMaster("local[1]"))
  println("App Master " + cong.equals(0))

  try {
    println("App Master " + cong.get("setAppName"))
  }
  catch {
    case e: Exception => println(e)
  }


}



