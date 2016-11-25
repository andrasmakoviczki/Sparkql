package model

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.Map

/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */
class Vertices(input:RDD[String],sc:SparkContext) {

  val tripleRDD: RDD[(String, String, String)] = input.map(line => {
    val l = line.split("\\s+")
    (l(0), l(1), l(2))
  })

  val nodes: RDD[String] = sc.union(tripleRDD.flatMap(x => List(x._1, x._3))).distinct

  val vertexIds: RDD[(String, VertexId)] = nodes.zipWithUniqueId

  val nodesIdMap: Map[String, VertexId] = vertexIds.collectAsMap()

  val vertexes: RDD[(String, RDFVertex)] = vertexIds.map(iri_id => {
    (iri_id._1, new RDFVertex(iri_id._2, iri_id._1))
  })

  val vertexRDD: RDD[(Long, RDFVertex)] =
    tripleRDD.map(triple => (triple._1, (triple._2, triple._3)))
      .join(vertexes)
      .map(f = t => {
        val p_o = t._2._1
        val vertex = t._2._2
        if (p_o._2(0) == '"') {
          vertex.addProps(p_o._1, p_o._2)
          //type property
        } else if (p_o._1 == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") {
          vertex.addProps(p_o._1, p_o._2)
          //object property
        } else {

        }
        (vertex.id, vertex)
      }).distinct

  def unpersistLocal(): Unit ={
    this.tripleRDD.unpersist()
    this.nodes.unpersist()
    this.vertexes.unpersist()
  }

  def getNodeIdMap = nodesIdMap
  def getTripleRDD = tripleRDD
  def getVertexRDD = vertexRDD

}
