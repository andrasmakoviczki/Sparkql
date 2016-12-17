import java.util.Calendar

import model._
import plan._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, Pregel, _}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */
object Evaluation {

  var fileName: String = ""
  var query: String = ""
  var finalResult = 0

  def show(x: Option[Long]) = x match {
    case Some(s) => s
    case None => 0L
  }


  //
  def vertexProgram(id: VertexId, attr: RDFVertex, msgSum: mutable.LinkedHashMap[String, RDFTable]): RDFVertex = {
    attr.mergeMsgToTable(msgSum)
    attr.iter = attr.iter + 1
    attr.clone()
  }

  //a merge b
  def msgCombiner(a: mutable.LinkedHashMap[String, RDFTable], b: mutable.LinkedHashMap[String, RDFTable]): mutable.LinkedHashMap[String, RDFTable] = {

    val head = mutable.LinkedHashMap[String, Int]()
    val rows = mutable.ArrayBuffer[mutable.ListBuffer[String]]()
    val emptyTable = new RDFTable(head, rows)

    b.foreach(i => {
      if (a.contains(i._1)) {
        a(i._1) = a.getOrElseUpdate(i._1, emptyTable).merge(i._2) //.clone()
      } else {
        a(i._1) = i._2 //.clone()
      }
    })
    a
  }

  def handleArguments(args: Array[String]): Unit = {
    if (args.length == 3) {
      fileName = args(0)
      query = args(2)
    }
  }

  def processResult(result: Graph[RDFVertex, String], planRes: PlanResult) = {
    val plan = planRes.plan
    val rootNode = planRes.rootNode
    var rowNum = 0

    val withResult = true
    if (!withResult) {
      println("WITHOUT RESULT " + Calendar.getInstance().getTime)
      System.exit(1)
    }
    println("RES " + Calendar.getInstance().getTime)

    val rootNodeDataProp = planRes.dataProperties.getOrElse(rootNode, new mutable.MutableList[VertexProperty]())

    if (plan.isEmpty) {
      val res = result.vertices.filter(v => v._2.checkDataProperty(rootNodeDataProp))
      println("RESULT1: " + res.count())
    } else {
      println("before REsult:" + result.vertices.count())
      println("plan size: " + plan.size)
      println("root node: " + rootNode)
      var res2 = result.vertices.filter(v => (v._2.tableMap.contains(rootNode)
        && (v._2.tableMap(rootNode).iteration.size == plan.size)
        && v._2.tableMap(rootNode).rows.nonEmpty
        ))
      println("before2 REsult:" + res2.count())

      res2 = res2.filter(v => v._2.checkDataProperty(rootNodeDataProp))
      if (res2.count() > 0) {
        res2.collect().foreach(v => {
          rowNum = rowNum + v._2.tableMap(rootNode).rows.size

        })
        println("RESULT2: " + rowNum)
      } else {
        println("RESULT3: 0")
      }
    }

    rowNum
  }

  def run(sc: SparkContext, args: Array[String]) {
    println("TIME START " + Calendar.getInstance().getTime)
    handleArguments(args)

    println("TIME READ VERTEX START " + Calendar.getInstance().getTime)

    //create vertex
    val input = sc.textFile(args(0))

    val vertices = new Vertices(input, sc)
    val vertexRDD = vertices.vertexRDD

    vertexRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val edges = new Edges(vertexRDD, vertices.tripleRDD, vertices.nodesIdMap)
    var edgeRDD = edges.edgeRDD

    edgeRDD.persist(StorageLevel.MEMORY_AND_DISK)

    vertices.unpersistLocal()
    edges.unpersistLocal

    println("QUERY: " + query)
    println("TIME CREATE PLAN START " + Calendar.getInstance().getTime)

    val planRes = new Plan(query)
    val plan = planRes.planRes.plan

    if (plan.nonEmpty) {
      val allP = plan(0).map(triple => {
        triple.tp.p
      })
      edgeRDD = edgeRDD.filter(edge => allP.contains(edge.attr))
    } else {
      edgeRDD = sc.emptyRDD[Edge[String]]
    }
    println("TIME EDGE UNION START " + Calendar.getInstance().getTime)
    edgeRDD = edgeRDD.union(edges.edgeLoopRDD)

    println("TIME CREATE GRAPH START " + Calendar.getInstance().getTime)

    val graph = Graph(vertexRDD, edgeRDD)
    println("TIME CREATE GRAPHOPS START " + Calendar.getInstance().getTime)

    val initMsg = mutable.LinkedHashMap.empty[String, RDFTable]


    def sendMsg(edge: EdgeTriplet[RDFVertex, String]): Iterator[(VertexId, mutable.LinkedHashMap[String, RDFTable])] = {
      val initMsg = mutable.LinkedHashMap.empty[String, RDFTable]

      //adott csúcs hanyadik iterációja
      var iteration = edge.dstAttr.getIter
      if (edge.srcAttr.iter > edge.dstAttr.iter) {
        iteration = edge.srcAttr.getIter
      }

      //Csúcshoz tartozó iterátora gráfban
      var i: Iterator[(VertexId, mutable.LinkedHashMap[String, RDFTable])] = Iterator.empty
      var i_withoutAlive: Iterator[(VertexId, mutable.LinkedHashMap[String, RDFTable])] = Iterator.empty

      //eml: plan: lekérdezések sorai vannak benne tripletekre felbontva
      if (iteration < plan.length) {
        //Veszi az aktuális elemet a planből és minden tripletre
        plan(iteration).foreach(triple => {

          val triplePattern = triple.tp
          val tablePattern = triple.headPattern
          //hurokélre (minek a hurokél?) és az él src-jének property-jei között van a plan aktuális sorának predicatje
          if (edge.attr == "LOOP" && edge.srcAttr.props.exists(vp => {vp.prop == triplePattern.p})) {
            if (triple.src == " ") {
              i = i ++ Iterator((edge.dstAttr.id, initMsg))
            } else {
              val m = edge.dstAttr.mergeEdgeToTable(
                triplePattern.s, triplePattern.o,
                triplePattern.s, triplePattern.o, edge.srcAttr.uri, edge.srcAttr.props.find(vp => {
                  vp.prop == triplePattern.p
                }).get.obj, iteration)
              i = i ++ Iterator((edge.srcAttr.id, m))
              i_withoutAlive = i_withoutAlive ++ Iterator((edge.srcAttr.id, m))
            }
            //nem hurok él és az él attribútuma a predicate
          } else if (edge.attr == triplePattern.p) {
            //ALIVE message
            if (triple.src == " ") {
              i = i ++ Iterator((edge.dstAttr.id, initMsg))
              //SEND forward
            } else if (triple.src == triplePattern.s) {
              val l =
              if (tablePattern.forall(a => edge.srcAttr.tableMap.contains(triplePattern.s) && edge.srcAttr.tableMap(triplePattern.s).head.contains(a)) &&
                edge.srcAttr.checkDataProperty(planRes.dataProperties.getOrElse(triplePattern.s, new mutable.MutableList[VertexProperty]())) &&
                edge.srcAttr.checkObjectProperty(triplePattern.s) && edge.dstAttr.checkObjectProperty(triplePattern.o)) {
                val m = edge.srcAttr.mergeEdgeToTable(
                  triplePattern.o, triplePattern.s,
                  triplePattern.s, triplePattern.o, edge.srcAttr.uri, edge.dstAttr.uri, iteration)
                i = i ++ Iterator((edge.dstAttr.id, m))
                i_withoutAlive = i_withoutAlive ++ Iterator((edge.dstAttr.id, m))
              }
              //SEND backward
            } else {
              if (tablePattern.forall(a => edge.dstAttr.tableMap.contains(triplePattern.o) && edge.dstAttr.tableMap(triplePattern.o).head.contains(a)) &&
                edge.dstAttr.checkDataProperty(planRes.dataProperties.getOrElse(triplePattern.o, new mutable.MutableList[VertexProperty]())) &&
                edge.dstAttr.checkObjectProperty(triplePattern.o) && edge.srcAttr.checkObjectProperty(triplePattern.s)) {
                val m = edge.dstAttr.mergeEdgeToTable(
                  triplePattern.s, triplePattern.o,
                  triplePattern.s, triplePattern.o, edge.srcAttr.uri, edge.dstAttr.uri, iteration)
                i = i ++ Iterator((edge.srcAttr.id, m))
                i_withoutAlive = i_withoutAlive ++ Iterator((edge.srcAttr.id, m))
              }
              i = i ++ Iterator((edge.dstAttr.id, initMsg))
            }
            //
          } else {
            //Iterator.empty
          }
        })
      } else {
      }
      i
    }

    val startTime = System.currentTimeMillis
    println("TIME PREGEL START " + Calendar.getInstance().getTime)
    val result = Pregel(graph, initMsg, Int.MaxValue, EdgeDirection.Either)(vertexProgram, sendMsg, msgCombiner)
    //println("AFTER PROCESS " + result.vertices.count())

    finalResult = processResult(result,planRes.planRes)

    val stopTime = System.currentTimeMillis
    println("TIME STOP " + Calendar.getInstance().getTime)
    println("Elapsed Time: " + (stopTime - startTime))
  }
}
