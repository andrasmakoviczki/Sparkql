package plan

import model.VertexProperty

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */
class PlanResult(_plan:ArrayBuffer[ListBuffer[PlanItem]], _rootNode:String, _varNum:Int, _dataProperties:mutable.LinkedHashMap[String,mutable.MutableList[VertexProperty]]) extends java.io.Serializable {
  val plan:ArrayBuffer[ListBuffer[PlanItem]] = _plan.clone()
  val rootNode:String = _rootNode
  val numVar:Int = _varNum
  val dataProperties: mutable.LinkedHashMap[String, mutable.MutableList[VertexProperty]] = _dataProperties.clone()
}
