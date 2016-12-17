package plan

import model.VertexProperty

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap, ListBuffer, MutableList}

/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */
class Plan(query: String) extends Serializable{

  def CreatePlan(sparkqlQuery: String) : Unit = {
    //object-ek szétválasztása
    //(Array[Triple],LinkedHashMap[String, MutableList[VertexProperty]])
    val (objPropPatterns,dataPropPatterns) = splitQuery(sparkqlQuery)
    //Init
    var rootNode = ""
    var vars = new mutable.HashSet[String]()
    val q = new mutable.Queue[String]
    val plan = new mutable.ArrayBuffer[PlanItem]()

    if(objPropPatterns.nonEmpty){
      rootNode = objPropPatterns(0).s
    } else {
      rootNode = dataPropPatterns.keySet.head
    }

    q.enqueue(rootNode)

    while(q.nonEmpty){
      val actVar = q.dequeue

//      for (pattern <- getNotProcessedPatterns(actVar,objPropPatterns){
//        vars.add(pattern.s)
//        vars.add(pattern.o)
//        if(pattern != actVar){
//          if(q.find(pattern.o) == null){
//            q.enqueue(pattern.o)
//          }
//          plan.append(PlanItem(pattern,pattern.o))
//        } else {
//          if(q.find(pattern.s) == null){
//            q.enqueue(pattern.s)
//          }
//          plan.append(PlanItem(pattern,pattern.o))
//        }
//        setPatternToProcessed(pattern)
//      }
    }
//
//    addAcceptedHeaders(plan)
//    addAliveMsgPatterns(plan)

    (plan,rootNode,vars.size,dataPropPatterns)
  }

  //ok
  val (tps, dataProperties):
    (Array[Triple], mutable.LinkedHashMap[String, mutable.MutableList[VertexProperty]]) = splitQuery(query)
  //ok
  var plan = ArrayBuffer[ListBuffer[PlanItem]]()
  //ok
  //osszes valtozo ebben van, ebben a plan generatorba, mert a q csak 1 elemet tartalmazhat
  val q = new mutable.Queue[String]()
  //ok
  var rootNode = ""
  //ok
  var vars: mutable.Set[String] = mutable.HashSet()

  var aliveTP = ArrayBuffer[ListBuffer[PlanItem]]()
  var iteration = ListBuffer[PlanItem]()
  var aliveIter = ListBuffer[PlanItem]()
  //az aktualis valtozo
  var v = ""
  var level: Int = 0

  if (tps.length > 0) {
    v = tps(0).s
    //rootNode kiválasztása a fához
    rootNode = tps(0).s

    vars = mutable.HashSet(tps(0).s, tps(0).o)
  } else {
    //rootNode kiválasztása a fához
    rootNode = dataProperties.keySet.head
  }

  val MsgPattern: mutable.LinkedHashMap[String, mutable.Set[String]] = mutable.LinkedHashMap[String, mutable.Set[String]]()

  //ha dataObject
  while (v != "") {
    //lementjük a változókat
    if (!MsgPattern.contains(v)) {
      MsgPattern(v) = mutable.HashSet()
    }
    //talaltunk megfelelo tp-t
    //Kell?
    var found: Boolean = false
    //kell meg ez a valtozo, mert van meg tp amiben szerepel
    var need: Boolean = false

    //minden triple-re (ami nem dataProperty)
    tps.foreach(tp => {
      //kigyűjti a változókat (s-o) helyekről
      vars += tp.s
      vars += tp.o

      //triple nem feldolgozott és a kiválasztott s-nél tartunk
      if(!tp.isFinished) {
          //melyeik helyen áll a változó
          v match {
            case tp.s => {
              if (!found) {
                if (!q.contains(tp.o)) q.enqueue(tp.o)
                aliveIter += new PlanItem(new Triple("?alive " + tp.p + " ?alive"), " ", mutable.Set[String]())
                iteration += new PlanItem(new Triple(tp.str), tp.o, mutable.Set[String]())
                tp.setFinished
              } else {
                need = true
              }
              found = true
          }
            case tp.o => {
              if (!found) {
                if (!q.contains(tp.s)) q.enqueue(tp.s)
                aliveIter += new PlanItem(new Triple("?alive " + tp.p + " ?alive"), " ", mutable.Set[String]())
                iteration += new PlanItem(new Triple(tp.str), tp.s, mutable.Set[String]())
                tp.setFinished
              } else {
                need = true
              }
              found = true
            }
            case _ => ;
        }
      }
    })

    //iteration feldolgozása
    if (iteration.nonEmpty) {
      aliveTP += aliveIter
      //első körben semmi
      if (aliveTP.length > 1) (0 until level).foreach {
        //iterációkhoz PlanItem
        aliveTP(_).map(iteration += _)
      }
      plan += iteration
    }

    //ürítés
    aliveIter = ListBuffer[PlanItem]()
    iteration = ListBuffer[PlanItem]()
    level = level + 1

    //következő változó
    if (!need) {
      if (q.nonEmpty) {
        v = q.dequeue
      } else {
        //leállási feltétel
        v = ""
      }
    }
  } //while END


  plan = plan.reverse

  //plan minden elemére
  plan = plan.map(iter => {
    //iter minden planItem-ére
    iter.map(planitem => {
      //ha nem segédél
      if (planitem.tp.s != "?alive") {
        val o = planitem.tp.o
        var src = planitem.src
        var dst = o
        //nyilak megfordítása a fában
        if (src == o) {
          dst = planitem.tp.s
        }

        //hozzávesszük a dst halmazához az src halmazát
        MsgPattern(dst) = MsgPattern(dst).union(MsgPattern(src))
        //hozzáadjuk az src-t is (redundáns?)
        MsgPattern(dst) += src

        //header elkészítése
        var mp = mutable.Set[String]()
        //
        MsgPattern(src).foreach(i => mp += i)
        planitem.headPattern = mp
        planitem
      } else {
        //segédélnél üreshalmaz a header
        planitem.headPattern = mutable.Set[String]()
        planitem
      }
    })
  })

  val planRes = new PlanResult(plan, rootNode, vars.size, dataProperties)

  def splitQuery(SparqlQuery: String): (Array[Triple], mutable.LinkedHashMap[String, mutable.MutableList[VertexProperty]]) = {
    val query: Array[String] = SparqlQuery.split(" . ")
    //Triple tömb létrehozása
    var TPs: Array[Triple] = query.map(tp => new Triple(tp))

    val dataProperties: mutable.LinkedHashMap[String, mutable.MutableList[VertexProperty]] = mutable.LinkedHashMap[String, mutable.MutableList[VertexProperty]]()

    TPs = TPs.flatMap(tp => {
      if ((tp.p == "rdf:type") || (tp.p == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") || tp.o(0) == '"') {
        //Lekéri a propListát, ha létezik, különben létrehozza az üres listát
        var propList = dataProperties.getOrElseUpdate(tp.s, mutable.MutableList[VertexProperty]())
        //Hozzáadja az aktuális (p-o)-t
        propList += new VertexProperty(tp.p, tp.o)
        //Frissítjük az s kulcshoz tartozó listát
        dataProperties(tp.s) = propList
        //Kiszedi az elemet
        Array[Triple]()
      } else {
        //Skip (Lehet helyettesíteni?)
        Array[Triple](tp)
      }
    })

    (TPs, dataProperties)
  }

  def planOut(p: ArrayBuffer[ListBuffer[PlanItem]]) : Unit = {
    p.foreach(list => {
      println("PLAN-- [")
      list.foreach(tp => println("PLAN-- (" + tp.tp.toString() + ", " + tp.src + ", " + tp.headPattern + ")"))
      println("PLAN-- ],")
    })
  }

  def dataPropertiesOut(dp : LinkedHashMap[String,MutableList[VertexProperty]]) : Unit = {
    println("PLAN-- DATAPROPERTIES")
    dp.foreach(v => {
      println("PLAN-- " + v._1)
      v._2.foreach(p => {
        println("PLAN-- " + p.prop + " " + p.obj)
      })
    })
  }

}
