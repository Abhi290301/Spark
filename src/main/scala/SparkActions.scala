import org.apache.spark.{SparkConf, SparkContext}

object SparkActions {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2,3]")
      .setAppName("Action Spark")

    println("Spark Session object Created successfully.")
    val sc = new SparkContext(conf)
    println("Spark Context Created Successfully.")
    var rdd = sc.parallelize(List(("Abhishek", 10000), ("Chandel", 20000)))
    rdd.foreach(f => {
      println(f)
    })

    val rddList = sc.parallelize(List(1,2,2,6,5))
    def param0 = (accum:Int,v:Int)=>accum+v
    def param1 = (accum1:Int,accum2:Int)=>accum1+accum2
    println("Aggregate : "+rddList.aggregate(0)(param0,param1))


    val inputRDD = sc.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))

    def param5 = (ac: Int ,v: (String, Int)) => ac + v._2
    def param6 = (acc : Int , acc1 : Int) =>acc + acc1
    println("Aggregate"+rdd.aggregate(0)(param5,param6))

    def param3 = (accu: Int, v: (String, Int)) => accu + v._2

    def param4 = (accu1: Int, accu2: Int) => accu1 + accu2

    println("aggregate : " + inputRDD.aggregate(0)(param3, param4))

    println("Tree aggregate : "+ inputRDD.treeAggregate(0)(param5,param6))

    println("fold: "+rddList.fold(0){
      (accum,v)=>
        val sum = accum + v
        sum
    })
      println ("fold :  " + rddList.fold(0) { (acc, v) =>
      val sum = acc + v
      sum
    })


    //Tree Reduce

    println("treeReduce : "+rddList.treeReduce(_ + _))


    //take & takeOrdered & takeSample
    println("Take : " +rddList.take(2).mkString(","))

    println("Take Ordered : " +rdd.takeOrdered(1).mkString(","))
    println("Take Sample: " +rddList.takeOrdered(2).mkString(","))








    sc.stop()
    println("1st Context Stopped")
  }
}
