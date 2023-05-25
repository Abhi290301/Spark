package Map_reduce_zip_z_parrallel

object ReduceEx {
  def main(args: Array[String]): Unit = {
    var str = List("Abhishek","Nitika","Chandel")
    var result1 = str.reduce((a,b)=>a+b)

    println(result1)
    println(result1.size)

    //find max no. in the list
    var max = Array(1,2,3,4)
    var result2 =max.reduce((x,y)=>x max y)
    println(result2)

    //find max no. in the list
    var min = Array(1, 2, 3, 4)
    var result3 = max.reduce((x, y) => x min y)
    println(result3)
  }
}
