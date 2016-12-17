package util

import scala.collection.mutable

/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */
class MyHashMap[A, B](initSize: Int) extends mutable.HashMap[A, B] {
  override def initialSize: Int = initSize // 16 - by default
}
