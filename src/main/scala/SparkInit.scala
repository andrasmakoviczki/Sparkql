import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */
object SparkInit {

  def isWindows : Boolean = System.getProperty("os.name").toLowerCase() contains "win"

  def initialize(): SparkContext ={
    val master = "local"
    val conf = new SparkConf()
      .setAppName("Spar(k)ql")

    if(isWindows) {
      conf.setMaster(master)
      System.setProperty("hadoop.home.dir", "c:\\Program Files (x86)\\Apache\\hadoop-2.6.0")
    }

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    sc
  }
}
