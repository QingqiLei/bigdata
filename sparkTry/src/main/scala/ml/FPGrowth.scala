package ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth

object FPGrowth {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("FPGROWTH")
    val sc = new SparkContext(conf)

    val transactions = sc.textFile(Thread.currentThread().getContextClassLoader().getResource("").getPath()+"/fpgrowth.txt").map(_.split(" ")).cache()

    println(s"交易样本的数量为： ${transactions.count()}")
    //最小支持度
    val minSupport = 0.4

    //计算的并行度
    val numPartition = 2

    //训练模型
    val model = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)
      .run(transactions)

    //打印模型结果
    println(s"经常一起购买的物品集的数量为： ${model.freqItemsets.count()}")
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    sc.stop()


  }
}
