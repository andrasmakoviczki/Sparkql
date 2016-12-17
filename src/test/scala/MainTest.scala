/**
  * Created by Andras Makoviczki on 2016. 11. 17..
  */

class MainTest extends org.scalatest.FunSuite with com.holdenkarau.spark.testing.SharedSparkContext {
  def isWindows : Boolean = System.getProperty("os.name").toLowerCase() contains "win"

  if(isWindows){
    System.setProperty("hadoop.home.dir", "c:\\Program Files (x86)\\Apache\\hadoop-2.6.0\\")
    //conf.set("spark.driver.allowMultipleContexts", "true")
  }

  val file = "src/main/resource/LUBM_1000.n3"

  test("Test1") {
    val query = "?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#GraduateStudent> ." +
      " ?X <http://spark.elte.hu#takesCourse> <http://www.Department0.University1000.edu/GraduateCourse0>"
    val args = Array(file,"",query)
    val expected = 3
    Evaluation.run(sc, args)
    val result = Evaluation.finalResult
    assert(result === expected)
  }

  test("Test2") {
    val query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#GraduateStudent> . " +
      "?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#University> . " +
      "?Z <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Department> . " +
      "?X <http://spark.elte.hu#memberOf> ?Z . ?Z <http://spark.elte.hu#subOrganizationOf> ?Y . " +
      "?X <http://spark.elte.hu#undergraduateDegreeFrom> ?Y "
    val args = Array(file,"",query)
    val expected = 4
    Evaluation.run(sc, args)
    val result = Evaluation.finalResult
    assert(result === expected)
  }

  test("Test3") {
    val query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Publication> . " +
      "?X <http://spark.elte.hu#publicationAuthor> <http://www.Department0.University1000.edu/AssistantProfessor0>"
    val args = Array(file,"",query)
    val expected=8
    Evaluation.run(sc, args)
    val result = Evaluation.finalResult
    assert(result === expected)
  }

  test("Test4") {
    val query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Professor> . " +
      "?X <http://spark.elte.hu#worksFor> <http://www.Department0.University1000.edu> . " +
      "?X <http://spark.elte.hu#name> ?Y1 . " +
      "?X <http://spark.elte.hu#emailAddress> ?Y2 . " +
      "?X <http://spark.elte.hu#telephone> ?Y3"
    val args = Array(file,"",query)
    val expected=27
    Evaluation.run(sc, args)
    val result = Evaluation.finalResult
    assert(result === expected)
  }

  test("Test5") {
    val query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Person> . " +
      "?X <http://spark.elte.hu#memberOf> <http://www.Department0.University1000.edu>"
    val args = Array(file,"",query)
    val expected=373
    Evaluation.run(sc, args)
    val result = Evaluation.finalResult
    assert(result === expected)
  }

  test("Test6") {
    //sc.setLogLevel("DEBUG")
    val query = "?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student>"
    val args = Array(file,"",query)
    val expected = 23734
    Evaluation.run(sc, args)
    val result = Evaluation.finalResult
    assert(result === expected)
  }

  test("Test7") {
    val query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . " +
      "?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Course> . " +
      "?X <http://spark.elte.hu#takesCourse> ?Y . " +
      "<http://www.Department0.University1000.edu/AssociateProfessor0> <http://spark.elte.hu#teacherOf> ?Y"
    val args = Array(file,"",query)
    val expected=52
    Evaluation.run(sc, args)
    val result = Evaluation.finalResult
    assert(result === expected)
  }

  test("Test8") {
    val query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . " +
      "?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Department> . " +
      "?X <http://spark.elte.hu#memberOf> ?Y . " +
      "?Y <http://spark.elte.hu#subOrganizationOf> <http://www.University1000.edu> . " +
      "?X <http://spark.elte.hu#emailAddress> ?Z"
    val args = Array(file,"",query)
    val expected=12725
    Evaluation.run(sc, args)
    val result = Evaluation.finalResult
    assert(result === expected)
  }

  test("Test9") {
    val query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . " +
      "?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Faculty> . " +
      "?Z <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Course> . " +
      "?X <http://spark.elte.hu#advisor> ?Y . " +
      "?Y <http://spark.elte.hu#teacherOf> ?Z . " +
      "?X <http://spark.elte.hu#takesCourse> ?Z"
    val args = Array(file,"",query)
    val expected=310
    Evaluation.run(sc, args)
    val result = Evaluation.finalResult
    assert(result === expected)
  }

  test("Test10") {
    val query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . " +
      "?X <http://spark.elte.hu#takesCourse> <http://www.Department0.University1000.edu/GraduateCourse0>"
    val args = Array(file,"",query)
    val expected=3
    Evaluation.run(sc, args)
    val result = Evaluation.finalResult
    assert(result === expected)
  }
}

