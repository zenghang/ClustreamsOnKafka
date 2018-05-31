import java.io.{IOException, PrintWriter}

object Generater {
  def main(args: Array[String]): Unit = {
    val path = "/home/hadoop/clustream/dataset/data.txt"
    val dataNum = 200000
    val writer : PrintWriter = new PrintWriter(path,"UTF-8")
    try{
      for (i <- 1 to dataNum){
        val str = (scala.util.Random.nextInt(40)+10).toString +","+ (scala.util.Random.nextInt(40)+10).toString +","+ (scala.util.Random.nextInt(40)+10).toString
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
