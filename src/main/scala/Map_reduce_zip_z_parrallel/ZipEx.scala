package Map_reduce_zip_z_parrallel

object ZipEx {
  def main(args: Array[String]): Unit = {
    var list1 = List("A","B","C")
    var list2 = List(1,2,3)

    var result = list2 zip list1
    println(result)
  }
}
