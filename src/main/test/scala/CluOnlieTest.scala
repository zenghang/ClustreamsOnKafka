import java.io._
import java.nio.file.{Files, Paths}
import java.util.concurrent.{Executors, TimeUnit}

import org.junit.Test
import org.kafkastreams.clustream._
import breeze.linalg._

class CluOnlieTest {
  @Test
  def TestgetSnaps: Unit ={
    val Clu : Clustream = new Clustream()
    println(Clu.getSnapShots("/home/hadoop/clustream/snap",10,9))
    val snap1 = Clu.getMCsFromSnapshots("/home/hadoop/clustream/snap",10,9)
    println(snap1.map(a => a.getN).mkString("[",",","]"))
    println("mics points = " + snap1.map(_.getN).sum)
    val clusters1 = Clu.fakeKMeans(4,10,snap1,3)
    if(clusters1 != null) {
      println("MacroClusters Ceneters")
      println("snapshots " + Clu.getSnapShots("/home/hadoop/clustream/snap",10,9))
      for (i <- 0 until clusters1.length){
        println("Cf1:"+clusters1(i).cf1x+"  N:"+clusters1(i).getN+"  Center:"+clusters1(i).getCenter)
      }
    }
    for(i <- 1 to 10) {
        if(Files.exists(Paths.get("/home/hadoop/clustream/snap"+ "/" + i)))
        try {
          val file = new ObjectInputStream(new FileInputStream("/home/hadoop/clustream/snap" + "/" + i))
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


