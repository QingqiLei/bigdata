import org.apache.spark.{SparkConf, SparkContext}
object WordCount extends App{
  val sparkConf = new SparkConf().setAppName("word").setMaster("spark://spark-1:7077")
    .setJars(List("/usr/local/JavaCode/bigdata/sparkTry/target/sparkTry-1.0-SNAPSHOT.jar")).setIfMissing("spark.driver.host","172.18.0.1")
  val sparkcontext = new SparkContext(sparkConf)
  val file = sparkcontext.textFile("hdfs://hadoop-1:9000/hqltest.hql")

  val words = file.flatMap(_.split(" "))
  val wordsTuple = words.map((_,1))
  wordsTuple.reduceByKey(_+_)saveAsTextFile("hdfs://hadoop-1:9000/out7")
  sparkcontext.stop()
}
