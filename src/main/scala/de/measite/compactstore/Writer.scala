package de.measite.compactstore

import java.lang._

class Writer(key : Array[Class[_]], value : Array[Class[_]]) {

    def type2size(t : Class[_]) : Option[Int] = t match {
        case _ if (t == classOf[Byte])    => Some(1)
        case _ if (t == classOf[Short])   => Some(2)
        case _ if (t == classOf[Integer]) => Some(4)
        case _ if (t == classOf[Long])    => Some(8)

        case _ if (t == classOf[Float])  => Some(4)
        case _ if (t == classOf[Double]) => Some(8)

        case _ if (t == classOf[Character]) => Some(2)

        case _ if (t == classOf[Boolean]) => Some(1)

        case _ => None
    }

    def value2bytes(v : AnyRef) : Option[Array[Byte]] = v match {
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

    class Collector() {
        var min : Option[Array[Array[Byte]]] = None
        var max : Option[Array[Array[Byte]]] = None
        var count : Int = 0
        def size = {
            
        }
    }

    def write(
        values : java.util.TreeMap[_, _],
        outputFile : String
    ) = {
        
    }

}
