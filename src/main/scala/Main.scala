/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */
object Main extends App{
    val sc = SparkInit.initialize()
    Evaluation.run(sc, args)
}
