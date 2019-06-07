import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.spark._
import org.apache.spark.rdd.RDD

class Mapper(n: Int) extends Serializable{
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")

  def doSomeMappingOnDataSetAndLogIt(rdd: RDD[Int]): RDD[String] =
    rdd.map{ i =>
      log.warn("mapping: " + i)
      (i + n).toString
    }
}

object Mapper {
  def apply(n: Int): Mapper = new Mapper(n)
}

object app {
  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName("demo-app")
    val sc = new SparkContext(conf)

    log.warn("Hello demo")

    val data = sc.parallelize(1 to 3)

    val mapper = Mapper(1)

    val other = mapper.doSomeMappingOnDataSetAndLogIt(data)

    other.collect()

    log.warn("I am done")
  }
}
