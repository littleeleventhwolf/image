package com.image.util

import java.io.InputStream
import java.net.Socket

import org.apache.hadoop.io.BytesWritable
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.mutable.ArrayBuffer


class ImageReceiver(host: String,port: Int,storageLevel: StorageLevel) extends
                                        Receiver[BytesWritable](storageLevel) {
	override def onStart(): Unit = {
		new Thread("Image Socket") {
			setDaemon(true)

			override def run(): Unit = {
				receive()
			}
		}.start()
	}

	override def onStop(): Unit = {

	}

	def receive(): Unit = {
		var socket: Socket = null
		var in: InputStream = null
		try {
			System.out.println("Connecting to " + host + ":" + port)
			socket = new Socket(host, port)
			System.out.println("Connected to " + host + ":" + port)
			in = socket.getInputStream()
			val buf = new ArrayBuffer[Byte]()
			var bytes = new Array[Byte](1024)
			var len = 0
			while (-1 < len) {
				len = in.read(bytes)
				if (len > 0) {
					buf ++= bytes
				}
			}
			val bw = new BytesWritable(buf.toArray)
			System.err.println("byte:::::" + bw.getLength)
			store(bw)
			System.out.println("Stopped receiving")
			restart("Retrying connecting to " + host + ":" + port)
		} catch {
			case e: java.net.ConnectException =>
				restart("Error connecting to " + host + ":" + port, e)
			case t: Throwable =>
				restart("Error receiving data", t)
		} finally {
			if (in != null) {
				in.close()
			}
			if (socket != null) {
				socket.close()
				System.out.println("Closed socket to " + host + ":" + port)
			}
		}
	}
}