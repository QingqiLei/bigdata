package stream.source

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  override def onStart(): Unit = {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }
  override def onStop(): Unit = {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }
  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
      // Connect to host:port
      socket = new Socket(host, port)
      val reader = new BufferedReader(new
          InputStreamReader(socket.getInputStream(), StandardCharsets. UTF_8 ))
      userInput = reader.readLine()
      while(!isStopped && userInput != null) {
        // 传送出来
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()
      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}

object CustomReceiver{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("custom")
    val ssc = new StreamingContext(conf, Seconds(3))

    val lines = ssc.receiverStream(new CustomReceiver("localhost",9999))
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_,1))
    val wordCount = pairs.reduceByKey(_+_)
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
