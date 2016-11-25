package model

/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */
class Message(val vari:String, val tab:RDFTable) extends java.io.Serializable {
  var table: RDFTable = tab
  var variable: String = vari
}

