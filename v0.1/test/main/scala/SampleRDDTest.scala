/**
  * Created by Andras Makoviczki on 2016. 11. 17..
  */

class SampleRDDTest extends org.scalatest.FunSuite with com.holdenkarau.spark.testing.SharedSparkContext {
  test("Simple test") {
    val input = List("hi", "hi cloudera", "bye")
    val expected = List(List("hi"), List("hi", "cloudera"), List("bye"))
    assert(sc.parallelize(input).map(_.split(" ").toList).collect().toList === expected)
  }
}

class MainTest extends org.scalatest.FunSuite with com.holdenkarau.spark.testing.SharedSparkContext {
  test("Simple test") {
    val query = "?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#GraduateStudent> . ?X <http://spark.elte.hu#takesCourse> <http://www.Department0.University1000.edu/GraduateCourse0>"
    var str = Array("LUBM_1000.n3","",query)
    Main.main(str)
  }
}