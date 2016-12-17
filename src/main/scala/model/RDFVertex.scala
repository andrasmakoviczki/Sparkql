package model

import scala.collection.mutable

/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */
class RDFVertex(val Vid: org.apache.spark.graphx.VertexId, val u: String) extends Serializable {
    val id: org.apache.spark.graphx.VertexId = Vid
    var uri: String = u
    var props:Array[VertexProperty] = Array[VertexProperty]()
    ///?
    var tableMap:mutable.LinkedHashMap[String,RDFTable] = mutable.LinkedHashMap.empty[String,RDFTable]

    //var query:RDFTable = new RDFTable(mutable.LinkedHashMap.empty[String,Int], mutable.ArrayBuffer[mutable.ListBuffer[String]]())
    //var finish:Boolean = false
    //var table:RDFTable = new RDFTable(mutable.LinkedHashMap.empty[String,Int], mutable.ArrayBuffer[mutable.ListBuffer[String]]())

    var iter:Int = -1

    //
    def checkDataProperty(queryProp: mutable.MutableList[VertexProperty]):Boolean= {
      //var s:String = uri+" dataprop: "+queryProp.clone().size+" "+props.size
      val b: Boolean = queryProp.forall(qp => {
        props.exists(p => {
          //s += qp.prop+" vs "+p.prop+" "+qp.obj+" vs "+p.obj
          qp.prop == p.prop && qp.obj == p.obj
        })
      })
      b
    }

    def checkObjectProperty(_uriVar: String):Boolean= {
      if (_uriVar(0) != '?' && _uriVar != uri) false else true
    }

    def addProps(p: String,o: String) {
      if (!props.exists( pro => {pro.prop == p && pro.obj == o})) {
        props = props :+ new VertexProperty(p,o)
      }
    }

    //???
    /*def haveMsg():Boolean ={
      false
    }*/


    def mergeMsgToTable(msg:mutable.LinkedHashMap[String,RDFTable]) {
      //TODO: ez a foreach csak egyszer fut le, mert csak egy változót értékelünk ki egyszerre
      msg.foreach(m => {
        if (tableMap.contains(m._1)) {
          tableMap(m._1) = tableMap(m._1).merge(m._2)
        } else {
          tableMap(m._1) = m._2  //.clone()
        }
        //				println("MERGE MSG TABLENODE: "+m._1)
      })
      if (msg.nonEmpty)
        tableMap.filter(m => msg.contains(m._1))
    }

    def getIter:Int= iter

    def mergeEdgeToTable(vertexVar:String,mergeVar:String,var1:String,var2:String,s:String,o:String, iteration:Int):mutable.LinkedHashMap[String,RDFTable]= {
      //println("edgeMergeTo: "+uri+" "+s+" "+o)
      var t = new RDFTable(
        mutable.LinkedHashMap[String,Int](var1->0,var2->1),
        mutable.ArrayBuffer[mutable.ListBuffer[String]](
          mutable.ListBuffer[String](s,o)
        )
      )
      //TODO: ITT TISZTAZNI A SOK ITER VS ITERATION-T
      //iter = melyik iteracioban van, iteration = mely iteraciokban vett reszt az eredmeny, az osszes iteracioval rendelkezo jo nekunk
      //+			t.iter = iter
      t.iteration.add(iteration)
      if (tableMap.contains(mergeVar)) {
        if (tableMap(mergeVar).head.nonEmpty)
          t = t.merge(tableMap(mergeVar))
      } else {
        //nop
      }
      mutable.LinkedHashMap[String,RDFTable](vertexVar->t)
    }

    def mergeTable(vertexVar:String,t:RDFTable) {
      //println("RDFVertex merge:"+uri+"  "+t.rows)
      //+			if (t.iter>iter) iter = t.iter
      //println("MERGE TABLENODE: "+vertexVar)
      tableMap(vertexVar) = tableMap(vertexVar).merge(t)  //.clone()
    }

    override def clone():RDFVertex = {
      val x = new RDFVertex(id, uri)
      x.iter = this.iter
      tableMap.foreach(v => x.tableMap(v._1)=v._2.clone())
      x.props = props.clone()
      x
    }
    override def toString:String = {
      var str:String = id+" "+uri
      props.foreach(prop => str = str+" "+prop.prop+"##PropObj##"+prop.obj)
      str
    }
  }

