package stream.source

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.{Seconds, StreamingContext}

object updatestatebykey extends App{

  val updateFunc = (values: Seq[Int], state: Option[Int]) =>{
    val currentcount = values.foldLeft(0)(_+_)
    val previouscount = state.getOrElse(0)
    Some(currentcount + previouscount)
  }

  val conf = new SparkConf().setMaster("local[2]").setAppName("update")
  val ssc = new StreamingContext(conf, Seconds(3))
  ssc.checkpoint(".")
  val lines = ssc.socketTextStream("localhost",9999)
  val words = lines.flatMap(_.split(" "))
  val pairs = words.map(word => (word,1))
  val stateDStream = pairs.updateStateByKey[Int](updateFunc)
  stateDStream.print()
  ssc.start()
  ssc.awaitTermination()
}
