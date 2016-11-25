package model

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.Map

/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */
class Edges(vertexRDD: RDD[(Long, RDFVertex)],tripleRDD: RDD[(String, String, String)], NodesIdMap: Map[String, VertexId]) extends Serializable{

  def show(x: Option[Long]) = x match {
    case Some(s) => s
    case None => 0L
  }

  val edgeLoopRDD: RDD[Edge[String]] = vertexRDD.map(vertex => {
    Edge(vertex._1, vertex._1, "LOOP")
  })

  var edgeRDD: RDD[Edge[String]] =
    tripleRDD.map(s_p_o => {
      if (s_p_o._3(0) != '"' && s_p_o._2 != "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") {
        Edge(show(NodesIdMap.get(s_p_o._1)), show(NodesIdMap.get(s_p_o._3)), s_p_o._2)
      } else {
        Edge(0, 0, "")
      }
    }).distinct.filter(e => e.srcId != 0)

  edgeRDD = edgeRDD.union(edgeLoopRDD)

  def unpersistLocal: Unit ={
    edgeLoopRDD.unpersist()
  }

  def getEdgeRDD = edgeRDD
  def getEdgeLoop = edgeLoopRDD
}
