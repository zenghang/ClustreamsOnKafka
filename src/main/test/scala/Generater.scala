import java.io.{IOException, PrintWriter}

object Generater {
  def main(args: Array[String]): Unit = {
    val path = "/Users/hu/KStream/data.txt"
    val dataNum = 100000
    val writer : PrintWriter = new PrintWriter(path,"UTF-8")
    try{
      for (i <- 1 to dataNum){
        val str = scala.util.Random.nextInt(50).toString +","+ scala.util.Random.nextInt(100).toString +","+ scala.util.Random.nextInt(50).toString
        if (i==dataNum)
          writer.print(str)
        else
          writer.println(str)
      }
    }
    catch {
      case ex: IOException => ex.printStackTrace()
    }
    finally {
      writer.close()
    }
  }

}
