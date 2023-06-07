package main1

import org.apache.spark.sql.SparkSession

object PairRDD_Functions extends App {
  val spark = SparkSession.builder()
    .appName("Pair Function")
    .master("local[*]")
    .getOrCreate()

  val rdd1= spark.sparkContext.parallelize(List("XenonStack Azure","XenonStack DataOps"))
  rdd1.collect().foreach(f=>
  println(f))

  val flatRDD = rdd1.flatMap(_.split(" "))

  val pairRdd = flatRDD.map(f=>(f,1))
  pairRdd.foreach(println)
    //Pair Functions
  println("Using Distinct:")
  pairRdd.distinct()
    .foreach(println)
  println("Using SortByKey:")
  pairRdd.sortByKey()
    .foreach(println)

  println("Using Reduced By key:")
  pairRdd.reduceByKey((a,b)=>a+b)
    .foreach(println)

  println("Using Reduced By key Locally:")
  pairRdd.reduceByKeyLocally((a, b) => a + b)
    .foreach(println)
  println("Using Collect as Map:")
  pairRdd.collectAsMap()
    .foreach(println)

      println("values ==>")
      pairRdd.values.foreach(println)

  println("Count :" + pairRdd.count())

}
