package de.measite.compactstore

import java.io.File
import java.io.FileOutputStream
import java.net.HttpURLConnection
import java.net.URL
import java.io.LineNumberReader
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import java.io.InputStreamReader
import java.util.Comparator

object CellIdRewrite extends App {

    val downloadURL = "http://downloads.opencellid.org/cell_towers.csv.gz"
    val towersFile = new File("./cell_towers.csv.gz")

    if (!towersFile.exists || towersFile.length == 0) {
        println("Downloading opencellid database " + downloadURL)
        println("This can take some time...")
        val url = new URL(downloadURL)
        val connection = url.openConnection().asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("GET")

        val out = new FileOutputStream(towersFile)
        val in = connection.getInputStream()

        val buffer = new Array[Byte](/* page size x4 */ 4 * 4 * 1024)
        var canRead = true
        var i = 0
        while(canRead) {
            i += 1
            if (i % 100 == 0) print(".")
            val readCount = in.read(buffer)
            if (readCount > 0) {
                out.write(buffer, 0, readCount)
            } else {
                val b = in.read()
                if (b >= 0) {
                    out.write(b.toByte)
                } else {
                    canRead = false
                }
            }
        }

        in.close()
        out.close()
        println()
    }

    println("Input file " + towersFile.getName() + " (" + towersFile.length + " bytes)")

    println("Reading input")
    val map = new java.util.TreeMap[(Int,Int,Int,Int),(Double,Double)](new Comparator[(Int,Int,Int,Int)](){
        override def compare(
            l : (Int,Int,Int,Int),
            r : (Int,Int,Int,Int)
        ) : Int = {
            if (l._1 < r._1) return -1
            if (l._1 > r._1) return  1
            if (l._2 < r._2) return -1
            if (l._2 > r._2) return  1
            if (l._3 < r._3) return -1
            if (l._3 > r._3) return  1
            if (l._4 < r._4) return -1
            if (l._4 > r._4) return  1
            return 0
        }
    })

    val writer = new Writer(
        Array(classOf[Int],classOf[Int],classOf[Int],classOf[Int]),
        Array(classOf[Double],classOf[Double])
    )

    {
        def string2int(s : String) = java.lang.Long.parseLong(s).toInt
        def encodeKey(mcc : Int,mnc : Int,cid : Int,lac : Int) =
            writer.value2bytes(List(cid,lac,mcc,mnc)).get

        val in = new LineNumberReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(towersFile))))
        var line = in.readLine()
        // skip header
        if (line != null) line = in.readLine()
        while (line != null) {
            val entries = line.split(",")
            try {
                val (mcc,mnc,cid,lac) =
                    (string2int(entries(0)),string2int(entries(1)),string2int(entries(2)),string2int(entries(3)))
                val (lon,lat) =
                    (
                        java.lang.Double.parseDouble(entries(4)),
                        java.lang.Double.parseDouble(entries(5))
                    )
                map.put(
                    (cid,lac,mcc,mnc),
                    (lon,lat)
                )
                if (cid > 65535 && !map.containsKey((cid & 0xffff,lac,mcc,mnc))) {
                    map.put(
                        (cid & 0xffff,lac,mcc,mnc),
                        (lon,lat)
                    )
                }
                if (map.size % 100000 == 0) {
                     println("Entries: " + map.size)
                }
            } catch {
                case t : NumberFormatException => {
                    println("Invalid row")
                    t.printStackTrace()
                }
            }
            line = in.readLine()
        }
        in.close
    }

    println("Entries: " + map.size)

    println("Generating binary compact store (towers.bcs)")
    writer.write(map, "towers.bcs")

}
