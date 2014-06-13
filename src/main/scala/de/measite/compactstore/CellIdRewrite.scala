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

    val wlan1DownloadURL = "http://openbmap.org/latest/wifi/raw/wifi-input_raw2013.zip"
    val wlanFile1 = new File("./wifi2013.zip")

    val wlan2DownloadURL = "http://openbmap.org/latest/wifi/raw/wifi-input_raw.zip"
    val wlanFile2 = new File("./wifi2014.zip")

    val wlan3DownloadURL = "http://www.openwlanmap.org/db.tar.bz2"
    val wlanFile3 = new File("./wifi.tar.bz2")

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

    val writer = new Writer(
        Array(classOf[Integer],classOf[Integer],classOf[Integer],classOf[Integer]),
        Array(classOf[java.lang.Double],classOf[java.lang.Double])
    )

    val map = writer.newValueMap()

    {
        def string2int(s : String) = java.lang.Long.parseLong(s).toInt
        def encodeKey(mcc : Int,mnc : Int,cid : Int,lac : Int) =
            List(
                new Integer(lac),new Integer(cid),
                new Integer(mcc),new Integer(mnc))
            .map(e => writer.value2bytes(e).get)
            .toArray
        def encodeValue(lon : Double, lat : Double) =
            List(new java.lang.Double(lon), new java.lang.Double(lat))
            .map(e => writer.value2bytes(e).get)
            .toArray

        val in = new LineNumberReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(towersFile))))
        var line = in.readLine()
        // skip header
        if (line != null) line = in.readLine()
        while (line != null) {
            val entries = line.split(",")
            try {
                val (mcc,mnc,lac,cid) =
                    (string2int(entries(0)),string2int(entries(1)),string2int(entries(2)),string2int(entries(3)))
                val (lon,lat) =
                    (
                        java.lang.Double.parseDouble(entries(4)),
                        java.lang.Double.parseDouble(entries(5))
                    )
                val v = encodeValue(lon,lat)
                map.put(encodeKey(mcc,mnc,cid,lac),v)
                if (cid > 0xffff && !map.containsKey(encodeKey(mcc,mnc,cid & 0xffff,lac))) {
                    map.put(encodeKey(mcc,mnc,cid & 0xffff,lac),v)
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
