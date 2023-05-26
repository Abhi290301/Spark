package WordCount

import org.apache.spark.sql.SparkSession

object WordCountExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("WordCount")
      .getOrCreate()

     val rdd = spark.sparkContext.textFile("C:\\tmp\\data.txt")
    println("initial partitioning count"+rdd.getNumPartitions)

    val rddCheck = spark.range(5,20).toDF().rdd
    rddCheck.foreach(println)

    val repar = rdd.repartition(4)
    println("After Repartioning"+repar.getNumPartitions)

    repar.collect().foreach(println)
    //Step 1
    val rdd2 = rdd.flatMap(f => f.split(" "))
    rdd2.foreach(f => println(f))

    //Create a Tuple by adding 1 to each word
    val rdd3 = rdd2.map(m => (m, 1))
    rdd3.foreach(println)

    //Step 3 Filter

    val rdd4 = rdd3.filter(f=>f._1.startsWith("a"))
    rdd4.foreach(println)


    //ReduceBy transformation
    val rdd5 = rdd3.reduceByKey(_ + _)
    rdd5.foreach(println)

    //Swap word,count and sortByKey transformation
    val rdd6 = rdd5.map(a => (a._2, a._1)).sortByKey()
    println("Final Result")

    //Action - foreach
    rdd6.foreach(println)

    //Action - count
    println("Count : " + rdd6.count())

    //Action - first
    val firstRec = rdd6.first()
    println("First Record : " + firstRec._1 + "," + firstRec._2)

    //Action - max
    val datMax = rdd6.max()
    println("Max Record : " + datMax._1 + "," + datMax._2)

    //Action - reduce
    val totalWordCount = rdd6.reduce((a, b) => (a._1 + b._1, a._2))
    println("dataReduce Record : " + totalWordCount._1)
    //Action - take
    val data3 = rdd6.take(3)
    data3.foreach(f => {
      println("data3 Key:" + f._1 + ", Value:" + f._2)
    })

    //Action - collect
    val data = rdd6.collect()
    data.foreach(f => {
      println("Key:" + f._1 + ", Value:" + f._2)
    })


  }


}
