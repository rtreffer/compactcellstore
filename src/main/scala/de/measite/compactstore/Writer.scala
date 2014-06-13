package de.measite.compactstore

import java.lang._
import scala.collection.JavaConversions._
import java.io.File
import java.io.DataOutputStream
import java.io.FileOutputStream
import java.io.OutputStream
import java.util.Comparator

class Writer(key : Array[Class[_]], value : Array[Class[_]]) {

    def type2size(t : Class[_]) : Option[Int] = t match {
        case _ if (t == classOf[scala.Byte])     => Some(1)
        case _ if (t == classOf[java.lang.Byte]) => Some(1)
        case _ if (t == classOf[Short])   => Some(2)
        case _ if (t == classOf[Integer]) => Some(4)
        case _ if (t == classOf[Int])     => Some(4)
        case _ if (t == classOf[Long])    => Some(8)

        case _ if (t == classOf[Float])  => Some(4)
        case _ if (t == classOf[java.lang.Double]) => Some(8)
        case _ if (t == classOf[scala.Double]) => Some(8)

        case _ if (t == classOf[Character]) => Some(2)

        case _ if (t == classOf[Boolean]) => Some(1)

        case _ => None
    }

    def value2bytes(v : AnyRef) : Option[Array[scala.Byte]] = v match {
        case b : Byte => Some(Array(b))
        case s : Short => Some(Array((s >> 8).toByte, s.toByte))
        case i : Integer => Some(Array(
                (i >> 24).toByte, (i >> 16).toByte,
                (i >> 8).toByte, i.toByte
            ))
        case l : Long => Some(Array(
            (l >> 56).toByte, (l >> 48).toByte,
            (l >> 40).toByte, (l >> 32).toByte,
            (l >> 24).toByte, (l >> 16).toByte,
            (l >> 8).toByte, l.toByte
        ))

        case f : Float => value2bytes(new Integer(java.lang.Float.floatToIntBits(f)))
        case d : Double => value2bytes(new java.lang.Long(java.lang.Double.doubleToLongBits(d)))

        case c : Character => value2bytes(new java.lang.Short(c.toShort))

        case b : Boolean =>
            if (b.booleanValue()) {
                Some(Array(0.toByte))
            } else {
                Some(Array(1.toByte))
            }

        case _ => None
    }

    def serializeKey(values : List[AnyRef]) =
        values.map(value2bytes).map(_.get).toArray

    if (key.map(type2size(_)).exists(_ == None)) {
        throw new IllegalArgumentException("All key types must have constant size")
    }

    val keysize = key.map(type2size(_)).flatten.sum
    val valuesize = if (value.map(type2size(_)).exists(_ == None)) {
        None
    } else {
        Some(value.map(type2size(_)).flatten.sum)
    }

    def compare(l : Array[scala.Byte], r : Array[scala.Byte]) : Int = {
        var i = 0
        while (i < l.length) {
            val lv = l(i) & 0xff
            val rv = r(i) & 0xff
            if (lv < rv) return -1
            if (lv > rv) return  1
            i += 1
        }
        0
    }
    def amin(l : Array[Array[scala.Byte]], r : Array[Array[scala.Byte]]) = {
        l.zip(r).map(e => compare(e._1, e._2) match {
            case -1 => e._1
            case  0 => e._1
            case  1 => e._2
        })
    }
    def amax(l : Array[Array[scala.Byte]], r : Array[Array[scala.Byte]]) = {
        l.zip(r).map(e => compare(e._1, e._2) match {
            case -1 => e._2
            case  0 => e._1
            case  1 => e._1
        })
    }

    class Collector() {
        var min : Option[Array[Array[scala.Byte]]] = None
        var max : Option[Array[Array[scala.Byte]]] = None
        var vmin : Option[Array[Array[scala.Byte]]] = None
        var vmax : Option[Array[Array[scala.Byte]]] = None
        var count : Int = 0
        val entries = new collection.mutable.ArrayBuffer[(Array[Array[scala.Byte]],Array[Array[scala.Byte]])]
        def add(
            key : Array[Array[scala.Byte]],
            value : Array[Array[scala.Byte]]
        ) : scala.Boolean = {
            if (min == None) {
                // init
                min = Some(key)
                max = min
                vmin = Some(value)
                vmax = vmin
                count = 1
                entries.add((key,value))
                return true
            }

            // compute the new size diff if we add the entry
            val newCount = count + 1
            val newMin = amin(min.get, key)
            val newMax = amax(max.get, key)
            val newVMin = amin(vmin.get, value)
            val newVMax = amax(vmax.get, value)
            val binarySize =
                bytesNeeded(newMin, newMax) * newCount +
                bytesNeeded(newVMin, newVMax) * newCount

            if (binarySize <= 4096) {
                min = Some(newMin)
                max = Some(newMax)
                vmin = Some(newVMin)
                vmax = Some(newVMax)
                count = newCount
                entries.add((key,value))
                return true
            }

            false
        }

        def write(out : OutputStream) {
            val kming = min.get.map(_.map(_ & 0xff))
            val vming = vmin.get.map(_.map(_ & 0xff))
            val keyBytes = relevantBytes(min.get,max.get)
            val valueBytes = relevantBytes(vmin.get,vmax.get)
            entries.foreach(e => {
                val (k,v) = e
                k.zip(keyBytes).map(e => {
                    e._2.foreach(i => out.write(e._1(i) & 0xff))
                })
                v.zip(valueBytes).map(e => {
                    e._2.foreach(i => out.write(e._1(i) & 0xff))
                })
            })
        }

        def bytesNeeded(min : Array[Array[scala.Byte]], max : Array[Array[scala.Byte]]) : Int = {
            var count = 0
            var i = 0
            while (i < min.length) {
                val minv = min(i)
                val maxv = max(i)
                var add = false
                var j = 0
                while (j < minv.length) {
                    if (add || minv(j) != maxv(j)) {
                        add = true
                        count += 1
                    }
                    j += 1
                }
                i += 1
            }
            count
        }
        def relevantBytes(
            min : Array[Array[scala.Byte]],
            max : Array[Array[scala.Byte]]
        ) : Array[Array[Integer]] = {
            var i = 0
            val res = new Array[Array[Integer]](min.length)
            while (i < min.length) {
                val minv = min(i)
                val maxv = max(i)
                var add = false
                val index = collection.mutable.ArrayBuffer[Integer]()
                var j = 0
                while (j < minv.length) {
                    if (add || minv(j) != maxv(j)) {
                        add = true
                        index += j
                    }
                    j += 1
                }
                res(i) = index.toArray
                i += 1
            }
            res
        }
    }

    def write(
        values : java.util.TreeMap[Array[Array[scala.Byte]], Array[Array[scala.Byte]]],
        outputFile : String
    ) = {
        println("Analyzing data")
        val cols = new collection.mutable.ArrayBuffer[Collector]
        var col = new Collector()
        values.entrySet().foreach(entry => {
            if (!col.add(entry.getKey(), entry.getValue())) {
                // we can't add the entry, add another block
                cols += col
                col = new Collector()
                col.add(entry.getKey(), entry.getValue())
            }
        })
        cols += col
        println("Will needed " + cols.size + " 4k blocks")

        println("Writing header")
        val of = new File(outputFile)
        val out = new DataOutputStream(new FileOutputStream(of))
        out.writeInt(cols.size)
        cols.foreach(col => {
            out.writeInt(col.entries.size)
            col.min.get.foreach { a => out.write(a,0,a.length) }
            col.max.get.foreach { a => out.write(a,0,a.length) }
            col.vmin.get.foreach { a => out.write(a,0,a.length) }
            col.vmax.get.foreach { a => out.write(a,0,a.length) }
        })
        out.flush

        val written = of.length
        val padding = ((written + 4095) / 4096) * 4096 - written

        def pad = {
            out.flush
            val written = of.length
            val padding = ((written + 4095) / 4096) * 4096 - written
            out.write(new Array[scala.Byte](padding.toInt), 0, padding.toInt)
            out.flush
        }

        println(s"Header: $written bytes (required padding: $padding)")
        pad

        println("Writing data")
        cols.foreach(col => {
            col.write(out)
            pad
        })

        val finalSize = of.length
        println(s"Size: $finalSize bytes")

        out.close()
    }

    def newValueMap() : java.util.TreeMap[Array[Array[scala.Byte]],Array[Array[scala.Byte]]] =
        new java.util.TreeMap[Array[Array[scala.Byte]],Array[Array[scala.Byte]]](
            new Comparator[Array[Array[scala.Byte]]](){
                override def compare(
                    l : Array[Array[scala.Byte]],
                    r : Array[Array[scala.Byte]]
                ) : Int = {
                    var i = 0
                    while (i < l.length && i < r.length) {
                        val f = Writer.this.compare(l(i), r(i))
                        if (f != 0) return f
                        i += 1
                    }
                    0
                }
            })

}
