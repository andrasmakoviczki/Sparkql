import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */
object SparkInit {

  def initialize(): SparkContext ={
    val master = "local"
    val conf = new SparkConf()
      .setAppName("Spar(k)ql")
      .setMaster(master)
      .set("spark.driver.allowMultipleContexts", "true")
    System.setProperty("hadoop.home.dir", "c:\\Program Files (x86)\\Apache\\hadoop-2.6.0")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    sc
  }
}
