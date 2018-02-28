import java.io._
import java.nio.file.{Files, Paths}
import java.util.concurrent.{Executors, TimeUnit}

import org.junit.Test
import org.kafkastreams.clustream.{ClockTask, CluStreamOnline, Clustream, MicroCluster}
import breeze.linalg._

class CluOnlieTest {
  @Test
  def TestgetSnaps: Unit ={
    val Clu : Clustream = new Clustream()
    println(Clu.getSnapShots("/Users/hu/KStream/snaps",10,9))
    for(i <- 1 to 10) {
        if(Files.exists(Paths.get("/Users/hu/KStream/snaps"+ "/" + i)))
        try {
          val file = new ObjectInputStream(new FileInputStream("/Users/hu/KStream/snaps" + "/" + i))
          val mc = file.readObject().asInstanceOf[Array[MicroCluster]]
          var text: Array[String] = null
          file.close()
          if(mc != null) {
            mc.foreach(a => println(a.toString))
          }
          println("==========================================================================================================================================")

        }
        catch {
          case ex: IOException => println("Exception while reading files " + ex)
            null
        }
      }
  }

  @Test
  def TestTime : Unit = {
    val Clu = new CluStreamOnline(20,3,10)
    val timerPool = Executors.newScheduledThreadPool(1)
    timerPool.scheduleAtFixedRate(new ClockTask(Clu),1,1,TimeUnit.SECONDS)
    while(true){
      Thread.sleep(1000)
      println(Clu.getGlobalTime)
    }
//    Clu.saveSnapShotsToDisk("/Users/hu/KStream/snaps",1,2,10)
  }

  @Test
  def TestOnline : Unit = {

    val clustream : Clustream = new Clustream()
    clustream.startOnline()
    while(true){

    }
  }
}


