package stream.source

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount extends App{
//  val sparkConf = new SparkConf().setAppName("word").setMaster("spark://spark-1:7077")
//    .setJars(List("/usr/local/JavaCode/bigdata/sparkTry/target/sparkTry-1.0-SNAPSHOT.jar")).setIfMissing("spark.driver.host","172.18.0.1")


  val sparkConf = new SparkConf().setAppName("streamingwordcount").setMaster("local[4]")
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  val lines = ssc.socketTextStream("localhost",9999)

  val words = lines.flatMap(_.split(" "))

  val pairs = words.map((_,1));

  val result = pairs.reduceByKey(_+_)

  result.print()

  ssc.start()
  ssc.awaitTermination()
}
