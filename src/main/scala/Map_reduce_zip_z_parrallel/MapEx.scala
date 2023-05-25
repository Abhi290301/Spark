package Map_reduce_zip_z_parrallel

object MapEx {
  def main(args: Array[String]): Unit = {
    var maps = Map("Abhishek" -> 10,"Nitika" -> 20)
    println("The size of the Map is : "+maps.size)
    println("\nAfter adding the element :")
    maps += ("Anchal" -> 30,"Akash" -> 40)
    println("Size of the Map after the addition of element :" +maps.size)
    println("Head of the Map :" +maps.head)
    for((k,v) <-maps){
      print("Key : "+k)
      println(" Value :"+v )
    }
  }

}
