package de.measite.compactstore

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class ReadWriteIntegrationSpec extends FlatSpec with Matchers {

    def short2keyseq(s : Short) =
        TestStore.key(s).productIterator.toSeq.asInstanceOf[Seq[AnyRef]]
    def short2valuearray(s : Short) =
        TestStore.value(s).productIterator.toArray.asInstanceOf[Array[AnyRef]]

    "A compactstore" should "be created without errors" in {
        TestStore.create()
    }

    "A compactstore" should "retrieve correct values" in {
        TestStore.create()
        val reader = TestStore.newReader
        for (i <- 1 until 100) {
            reader.get(short2keyseq(i.toShort) : _*) should be (
                short2valuearray(i.toShort))
        }
    }

    "A compactstore" should "handle range scans" in {
        TestStore2.create()
        val reader = TestStore2.newReader

        // it should be possible to retrieve several entries for a given key
        // prefix
        for (i <- 0 until 256) {
            val c =
                (0 until 256).
                map(j => if ((i.toByte & j.toByte) == 0) { 1 } else { 0 }).
                foldLeft(0)(_+_)
            println(i + " -> " + c)
            reader.getAll(new java.lang.Byte(i.toByte)).size should be (c)
        }
    }

}
