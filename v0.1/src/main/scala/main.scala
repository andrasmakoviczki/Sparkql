/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */
object Main {
  def main(args: Array[String]): Unit = {
    val sc = SparkInit.initialize()
    Evaluation.run(sc,args)
  }
}
