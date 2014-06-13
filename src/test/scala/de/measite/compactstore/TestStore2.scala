package de.measite.compactstore

import java.io.File

/**
 * A TestStore util to create a teststore, and to generate key/value pairs.
 * Keys in the teststore consist of a byte and a short value, where the byte
 * is mixed from the short value (every second bit). The value is simply the
 * bitinverse of the short and the byte as given in the key.
 */
object TestStore2 {

    def create() {
        val filename =
            System.getProperty("java.io.tmpdir") + File.separator +
            "compactstore2.bcs"
        val file = new File(filename)

        if (file.exists) file.delete

        val w =
            new Writer(
                Array(classOf[java.lang.Byte],classOf[java.lang.Byte],classOf[java.lang.Byte]),
                Array(classOf[java.lang.Short],classOf[java.lang.Byte]))

        val values = w.newValueMap()

        var i = 0
        while (i < 65536) {
            val (k0,k1,k2) = key(i.toShort)
            val k = k0 & k1
            if (k == 0)
                values.put(
                    w.serializeKey(List(k0,k1,k2)),
                    w.serializeKey(value(i.toShort).productIterator.toList.asInstanceOf[List[AnyRef]]))
            i += 1
        }

        println("Entries: " + values.size)
        w.write(values, filename)

    }

    def key(s : Short) = 
        (
            new java.lang.Byte((s >> 8).toByte),
            new java.lang.Byte(s.toByte),
            new java.lang.Byte(((s & 0x5500 >> 7) | (s & 0x55)).toByte)
        )

    def value(s : Short) = {
        val (_,_,b) = key(s)
        var v = 0
        val k = s & 0xffff
        var i = 0
        while (i < 16) {
            v = v | (((k >> i) & 1) << (15 - i))
            i += 1
        }
        (new java.lang.Short(v.toShort),b)
    }

    def newReader() = {
        val filename =
            System.getProperty("java.io.tmpdir") + File.separator +
            "compactstore2.bcs"
        val file = new File(filename)
        if (!file.exists()) create()
        new Reader(
            Array(classOf[java.lang.Byte],classOf[java.lang.Byte],classOf[java.lang.Byte]),
            Array(classOf[java.lang.Short],classOf[java.lang.Byte]),
            filename)
    }

}
