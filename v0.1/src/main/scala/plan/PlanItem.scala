package plan

import scala.collection.mutable

/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */

class PlanItem(_tp: Triple, _src: String, _headPattern: mutable.Set[String]) extends java.io.Serializable {
  val tp: Triple = _tp
  val src: String = _src
  var headPattern: mutable.Set[String] = _headPattern
}
