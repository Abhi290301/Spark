import org.apache.spark.{SparkConf, SparkContext}

object SparkContextEx {

  def main(args: Array[String]): Unit = {
    try {
      val conf = new SparkConf().setAppName("Spark Programming").setMaster("local[1]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      println("Hii 1st Spark Context Running")
      println(sc)
      sc.stop()
      println("1st Context Stopped")

      val sc2 = new SparkContext(conf)
      sc2.setLogLevel("WARN")
      println("2nd Spark Context is ready to execute.")
      println("Version odf the Spark is " + sc2.version)
      val rdd = sc2.range(1, 5)
      rdd.foreach(println)

      //Parallelize
      val rdd2 = sc2.parallelize(List("Abhishek", 2, "Chandel"))
      //Converting list into the Array
      val arr: Array[Any] = rdd2.collect()
      println("No. of Partitions : " + rdd2.partitions(0))
      println("No. of Partitions : " + rdd2.getNumPartitions)
      println("Storage Level : " + rdd2.getStorageLevel)
      rdd2.foreach(println)

      //Reading the files
      val rdd3 = sc2.textFile("C:\\tmp\\text1.txt")

      rdd3.foreach(f => println(f))


      val rdd4 = sc2.parallelize(LazyList(("Abhishek", 1902031001), ("Akash", 1902031005)))

      rdd4.foreach(f => {
        println(f)
      })

      //Reading Multiple Files together
      val rdd5 = sc2.wholeTextFiles("C:\\tmp\\text1.txt,C:\\tmp\\text3.txt,C:\\tmp\\text3.txt")
      rdd5.foreach(f => {
        println(f._1 +"=>"+ f._2)
      })
      //Methods of RDD SparkContext
      println("Accumulator of long type: " + sc2.longAccumulator("Hii"))
      println("Deploy Mode : " + sc2.deployMode)
      println("Double Accumulator " + sc2.doubleAccumulator("Spark"))
      println("ChckPoint Directory " + sc2.getCheckpointDir)
      println("BroadCAst " + sc2.broadcast())
      println("Storage Info " + sc2.getRDDStorageInfo)
      println("Storage Info " + sc2.hadoopFile("rdd.txt"))
      println("Storage Info " + sc2.getRDDStorageInfo)


      //CSV files

      val rddCSV = sc2.textFile("C:\\tmp\\text1.csv")
      rddCSV.foreach(f => {
        println("Col1:" + f(0) + ",Col2:" + f(1))
      })



      }catch {
      case e :Exception =>println(e.printStackTrace())
    }
  }
}
