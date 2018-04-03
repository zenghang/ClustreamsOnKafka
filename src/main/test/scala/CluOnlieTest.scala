import java.io._
import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit}

import org.junit.Test
import org.kafkastreams.clustream.{CluStreamOnline, _}
import breeze.linalg._

class CluOnlieTest {
  @Test
  def TestgetSnaps: Unit ={
    val Clu : Clustream = new Clustream()
    val snapsPath = "/home/hadoop/clustream/snap1"
//    println(Clu.getSnapShots(snapsPath,17,15))
//    val snap1 = Clu.getMCsFromSnapshots(snapsPath,17,15)
//    println(snap1.map(a => a.getN).mkString("[",",","]"))
//    println("mics points = " + snap1.map(_.getN).sum)
//    val clusters1 = Clu.fakeKMeans(5,10000,snap1,3)
//    if(clusters1 != null) {
//      println("MacroClusters Ceneters")
//      //println("snapshots " + Clu.getSnapShots(snapsPath,17,15))
//      for (i <- 0 until clusters1.length){
//        println("Cf1:"+clusters1(i).cf1x+"  N:"+clusters1(i).getN+"  Center:"+clusters1(i).getCenter)
//      }
//    }

    //    for(i <- 2 until  12) {
    if(Files.exists(Paths.get(snapsPath+ "/" + 18)))
      try {
        val file = new ObjectInputStream(new FileInputStream(snapsPath + "/" + 18))
        val mcs = file.readObject().asInstanceOf[Array[MicroCluster]]
        println(mcs.map(a => a.getN).mkString("[",",","]"))
        println("mics points = " + mcs.map(_.getN).sum)
        var sq = 0.0
        for (mc <- mcs){
          val part1 = Clu.sumVector(mc.cf2x)
          val part2 = mc.n*Clu.sumVector(mc.getCenter :* mc.getCenter)
          val part3 = 2*Clu.sumVector(mc.getCenter :* mc.cf1x)
          val sum = part1 + part2 - part3
          sq = sq + sum
        }

        file.close()
        if(mcs != null) {
          mcs.foreach(a => println(a.toString))
        }
        println("The Quality is :"+sq)
        println("==========================================================================================================================================")

      }
      catch {
        case ex: IOException => println("Exception while reading files " + ex)
          null
      }
    //      }
  }

  @Test
  def TestTime : Unit = {
    val Clu = new CluStreamOnline(20,3,10)
    val timerPool = Executors.newScheduledThreadPool(1)
    timerPool.scheduleAtFixedRate(new ClockAndSaveTask(Clu),1,1,TimeUnit.SECONDS)
    while(true){
      Thread.sleep(1000)
      println(Clu.getGlobalTime)
    }
    //    Clu.saveSnapShotsToDisk("/Users/hu/KStream/snaps",1,2,10)
  }

  @Test
  def TestTime2 : Unit = {
    val time = new AtomicLong(1);
    time.set(0);
    println(time.longValue())
  }


  @Test
  def TestOnline : Unit = {

    val clustream : Clustream = new Clustream()
    clustream.startOnline()
    while(true){

    }
  }

}


