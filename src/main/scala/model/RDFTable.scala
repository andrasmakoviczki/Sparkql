package model

import scala.collection.mutable

/**
  * Created by Andras Makoviczki on 2016. 11. 24..
  */
class RDFTable(val _head:mutable.LinkedHashMap[String, Int],val _rows:mutable.ArrayBuffer[mutable.ListBuffer[String]]) extends Serializable {

    var head:mutable.LinkedHashMap[String, Int] = _head
    var rows:mutable.ArrayBuffer[mutable.ListBuffer[String]] = _rows

    var iteration:mutable.Set[Int] = mutable.Set.empty[Int]

    // Ellenőrzi, van e közös eleme a megadott táblával
    def checkUnion(table2:RDFTable):Boolean= {
      val s = ((head.keySet -- table2.head.keySet) ++ (table2.head.keySet -- head.keySet)).size
      s==0
    }

    //RDF téblék összevonása
    def merge(table2:RDFTable):RDFTable= {
      //var tmp = "merge: oldrow: "+rows+"  newrow: "+table2.rows
      val union = checkUnion(table2)
      if (head.isEmpty) {
        head = table2.head
        rows = table2.rows
      }

      val table1:RDFTable = this
      val newhead:mutable.LinkedHashMap[String,Int] = table1.head.clone()		//ez a clone kell!
      var newrows:mutable.ArrayBuffer[mutable.ListBuffer[String]] = mutable.ArrayBuffer[mutable.ListBuffer[String]]()

      if (union) {
        newrows = table1.rows		//.clone()
        table2.rows.foreach(r2 => {
          val newrow:mutable.ListBuffer[String] = mutable.ListBuffer[String]()
          table1.head.foreach( h => {
            newrow.append(r2(table2.head(h._1)))
          })
          newrows.append(newrow)
        })
      } else {
        newrows = table1.rows.flatMap(r => table2.rows.map(r2 => {
          var l = true
          table2.head.foreach( h2 => {
            if (table1.head.contains(h2._1) && (r(table1.head.getOrElseUpdate(h2._1,-1)) != r2(h2._2))) {
              l = false
            }
          }
          )
          if (l) {
            val newrow:mutable.ListBuffer[String] = r.clone()		//ez a clone kell
            table2.head.foreach( h2 => {
              if (!table1.head.contains(h2._1)) {
                newrow.append(r2(h2._2))
                if(!newhead.contains(h2._1)){
                  newhead(h2._1) = newhead.size
                }
              }
            })
            newrow
          } else {
            mutable.ListBuffer[String]()
          }
        }
        ))
      }
      newrows = newrows.filter(r => r.nonEmpty)
      table1.rows = newrows
      table1.head = newhead

      //println(tmp)

      /*var m = "table1 iter: ("
      table1.iteration.foreach(x => m = m+x)
      m = m + ") table2 iter: ("
      table2.iteration.foreach(x => m = m+x)
      println(m+") ")
      */
      table1.iteration = table1.iteration.union(table2.iteration)
      table1	//.clone()
    }

    override def clone():RDFTable= {
      this
      /*			var newhead:mutable.LinkedHashMap[String,Int] = this.head.clone()
            var newrow:mutable.ArrayBuffer[mutable.ListBuffer[String]] = mutable.ArrayBuffer[mutable.ListBuffer[String]]()
            this.rows.foreach(row => newrow += row.clone())
            var x:RDFTable = new RDFTable(newhead,newrow)
            x.iter = this.iter
            x.iteration = this.iteration.clone()
            return x*/
    }
}
