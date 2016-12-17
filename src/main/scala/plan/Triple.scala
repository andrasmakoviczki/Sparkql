package plan

/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */
class Triple(val tp:String) extends java.io.Serializable {
    //Megkapja a sort
    var spo: Array[String] = tp.split(" ")
    //Szétbontja
    val s:String = spo(0)
    val p:String = spo(1)
    val o:String = spo(2)
    val str:String = tp

    var finish:Boolean = false

    def isFinished : Boolean = {
        finish
    }

    def setFinished : Unit = {
        finish = true
    }

    override def toString:String ={
      str
    }
}
