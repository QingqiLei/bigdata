package stream.source

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
object QueueRdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("QueueRdd")
    val ssc = new StreamingContext(conf,Seconds(3))
    val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()
    val inputSteam = ssc.queueStream(rddQueue)
    val mappedStream = inputSteam.map(x => (x%10 ,1))
    val reduceStream = mappedStream.reduceByKey(_+_)
    reduceStream.print()
    //启动计算
    ssc.start()
    // Create and push some RDDs into
    for (i <- 1 to 30) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread. sleep (2000)
      //通过程序停止 StreamingContext 的运行
      //ssc.stop()
    }
  }
}
