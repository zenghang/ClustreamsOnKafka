import org.junit.Test
import org.kafkastreams.clustream.CluStreamOnline
import breeze.linalg._

class CluOnlieTest {
  @Test
  def TestUpdate: Unit ={
    val Clu = new CluStreamOnline(20,3,10)
    val data = Vector(Array(1.0,2.0,3.0))
    Clu.run(data)
  }

}
