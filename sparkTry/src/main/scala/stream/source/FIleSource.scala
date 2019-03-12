package stream.source

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FIleSource extends App {

  val sparkConf = new SparkConf().setAppName("streamingwordcount").setMaster("local[4]")
  val ssc = new StreamingContext(sparkConf, Seconds(3))
  val lines = ssc.textFileStream("hdfs://hadoop-1:9000/data/")

  val words = lines.flatMap(_.split(" "))

  val pairs = words.map((_,1));

  val result = pairs.reduceByKey(_+_)

  result.print()

  ssc.start()
  ssc.awaitTermination()

}
