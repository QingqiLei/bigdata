### 重点知识

#### 1 是什么

1. 是spark 中的一个组件, 基于spark core 进行构建, 用于对流式进行处理, 类似于storm
2. 能够和spark core, spark sql 进行混合编程
3. 能接受什么数据? kafka, flume, hdfs, twitter
4. 无状态的转换（前面处理的数据和后面处理的数据没啥关系）、有转换转换（前面处理的数据和后面处理的数据是有关系的，比如叠加关系）

#### 2 怎么实现的

1. 对于整个流式计算来说，数据流你可以想象成水流，微批次架构的意思就是将水流按照用户设定的时间间隔分割为多个水流段。一个段的水会在Spark中转换成为一个RDD，所以对水流的操作也就是对这些分割后的RDD进行单独的操作。每一个RDD的操作都可以认为是一个小的批处理（也就是离线处理）。

#### 3 dsfream 是什么

1、DStream是类似于RDD和DataFrame的针对流式计算的抽象类。在源码中DStream是通过HashMap来保存他所管理的数据流的。K是RDD中数据流的时间，V是包含数据流的RDD。
2、对于DStream的操作也就是对于DStream他所包含的所有以时间序列排序的RDD的操作。

## 例子

#### 加入依赖

```xml
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-streaming_2.11</artifactId>
        <version>2.4.0</version>
        <!--<scope>provided</scope>-->
    </dependency>
```

#### 运行程序

```scala
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
```

#### netcat

```shell
nc -lk 9999 
hello
```

### 数据源

#### 1 文件数据源

会监控dataDirectory 目录并不断处理移动进来的文件, 记住目前不支持嵌套目录

1) 文件需要有相同的数据格式
2) 文件进入 dataDirectory 的方式需要通过移动或者重命名来实现 。
3) 一旦文件移动进目录,则不能再修改,即便修改了也不会读取新数
据。
如果文件比较简单,则可以使用
streamingContext.textFileStream(dataDirectory)方法来读取文件。文件流不需要
接收器,不需要单独分配 CPU 核。

```scala
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
```



#### 2 自定义

1、你需要新建一个Class去继承Receiver，并给Receiver传入一个类型参数，该类型参数是你需要接收的数据的类型。
2、你需要去复写Receiver的方法： onStart方法（在Receiver启动的时候调用的方法）、onStop方法（在Receiver正常停止的情况下调用的方法）
3、你可以在程序中通过streamingContext.receiverStream( new CustomeReceiver)来调用你定制化的Receiver。

```scala
package stream

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

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
```

`nc -lk 9999`





#### 3 RDD 队列

你可以通过StreamingContext.queueStream(rddQueue)这个方法来监控一个RDD的队列，所有加入到这个RDD队列中的新的RDD，都会被Streaming去处理。

```scala
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
```

### 转换

#### 无状态转换

map 将原DStream 中的每一个元素通过函数func 从而得到新的DStream

flatMap 

filter 选择源DStream中函数func判为true的记录作为新DStream

repartition 通过创建更多或者更少的partition来改变此DStream的并行级别。

union 将一个具有相同slideDuration新的DStream和当前DStream进行合并，返回新的DStream

count 统计源DStreams中每个RDD所含元素的个数得到单元素RDD的新DStreams。

reduce  通过函数func来整合源DStreams中每个RDD元素得到单元素RDD的DStream。

countByValue 通过函数func(两个参数一个输出)来整合源DStreams中每个RDD元素得到单元素RDD的DStream。

reduceByKey  对(K,V)对的DStream调用此函数，返回同样（K,V)对的新DStream，但是新DStream中的对应V为使用reduce函数整合而来

join  两DStream分别为(K,V)和(K,W)对，返回(K,(V,W))对的新DStream。

cogroup  两DStream分别为(K,V)和(K,W)对，返回(K,(Seq[V],Seq[W])对新DStream

transform  将RDD到RDD映射的函数func作用于源DStream中每个RDD上得到新DStream。这个可用于在DStream的RDD上做任意操作。注意的是，在这个转换函数里面能够应用所有RDD的转换操作。

#### 有状态转换

updateStateByKey   使用 updateStateByKey 需要对检查点目录进行配置 ,会使用检查点来保存
状态。

updateFunc

updateStateByKey

window  基于对源DStream窗化的批次进行计算返回一个新的DStream，windowDuration是窗口大小，slideDuration滑动步长。

countByWIndow  注意，返回的是window中记录的条数。

recuceByWindow  通过使用自定义函数整合滑动区间流元素来创建一个新的单元素流。

reduceByKeyAndWindow   通过给定的窗口大小以滑动步长来应用reduceFunc函数，返回DStream[(K, V)], K就是DStream中相应的K，V是window应用了reduce之后产生的最终值。

reduceByKeyAndWindow

#### transform 

Transform 原语允许 DStream 上执行任意的 RDD-to-RDD 函数。即使这些
函数并没有在 DStream 的 API 中暴露出来,通过该函数可以方便的扩展 Spark
API。
该函数每一批次调度一次。
比如下面的例子,在进行单词统计的时候,想要过滤掉 spam 的信息。
其实也就是对 DStream 中的 RDD 应用转换。

```scala
val spamInfoRDD = ssc.sparkContext.newAPIHadoopRDD(...) // RDD containing spam information
val cleanedDStream = wordCounts.transform { rdd =>
rdd.join( spamInfoRDD ).filter(...) // join data stream with spam information to do data
cleaning
...
}
```

#### join

```scala
val stream1 : DStream[String, String] = ...
val stream2 : DStream[String, String] = ...
val joinedStream = stream1 .join( stream2 )
val windowedStream1 = stream1 .window(Seconds(20))
val windowedStream2 = stream2 .window(Minutes(1))
val joinedStream = windowedStream1 .join( windowedStream2 )
```

#### 累加器和广播变量

```scala
object WordBlacklist {
    @volatile private var instance : Broadcast[Seq[String]] = null
    def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
        if ( instance == null) {
            synchronized {
                if ( instance == null) {
                    val wordBlacklist = Seq ("a", "b", "c")
                    instance = sc.broadcast(wordBlacklist)
                }
            }
        }
        instance
    }
}
object DroppedWordsCounter {
    @volatile private var instance : LongAccumulator = null
    def getInstance(sc: SparkContext): LongAccumulator = {
        if ( instance == null) {
            synchronized {
                if ( instance == null) {
                    instance = sc.longAccumulator("WordsInBlacklistCounter")
                }
            }
        }
        instance
    }
}
wordCounts.foreachRDD { (rdd: RDD[(String, Int)], time: Time) =>
    // Get or register the blacklist Broadcast
    val blacklist = WordBlacklist. getInstance (rdd.sparkContext)
    // Get or register the droppedWordsCounter Accumulator
    val droppedWordsCounter = DroppedWordsCounter. getInstance (rdd.sparkContext)
    // Use blacklist to drop words and use droppedWordsCounter to count them
    val counts = rdd.filter { case (word, count) =>
        if (blacklist.value.contains(word)) {
            droppedWordsCounter.add(count)
            false
        } else {
            true
        }
    }.collect().mkString("[", ", ", "]")
    val output = "Counts at time " + time + " " + counts
})
```

### graphx 是什么

1. 是spark 中的一个模块, 主要用于以图为核心的计算还有分布式图的计算
2. graphx 他的底层也是RDD 计算, 和rdd 共用一种存储形态, 在展示形态上可以以数据集来表示, 也是以图的形式来表示

### 要学什么

1. 顶点 用`RDD[(VertexId, VD)] 来表示, (VertexId, VD)这个元组表示一个顶点, vertexid 表示id, 是long 类型, vd 是属性, 可以是任何类型`
2. 边用`rdd[edge[ed]]` 来表示, edge 用来具体表示一个边, edge 里面包含一个ed类型参数设定属性, 一个源顶点id 和一个目标顶点的id
3. 三元组: 用`RDD[EdgeTriplet[VD, ED]]`,EdgeTriplet[VD, ED]来表示一个三元组，三元组包含了一个边、边的属性、源顶点ID、源顶点属性、目标顶点ID、目标顶点属性。VD和ED是类型参数，VD表示顶点的属性，ED表示边的属性。

### 小试

### 创建

#### 1 顶点创建

* `users: RDD[(VertexId, (String, String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),(5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))`
* ` val users1:VertexRDD[(String, String)] = VertexRDD[(String, String)](users)`

#### 2 边的创建

* `  val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))`
* ` val relationships1:EdgeRDD[String] = EdgeRDD.fromEdges(relationships)`

#### 3 图的创建

* `Graph[VD: ClassTag, ED: ClassTag]` `def apply[VD: ClassTag, ED: ClassTag]`
* graph2 = graph.fromEsges(relationships, defaultUser)  对于顶点的属性是使用提供的默认属性。
* 对于顶点的属性是使用提供的默认属性，对于边的属性是相同边的数量。`val relationships: RDD[(VertexId,VertexId)] = sc.parallelize(Array((3L, 7L),(5L, 3L),(2L, 5L), (5L, 7L)))
  ​      val graph3 = Graph.fromEdgeTuples[(String,String)](relationships,defaultUser)`

#### 输出

`graph2.vertices.collect.foreach(println _)`

`graph2.edges.collect.foreach(println _)`



### 转换

1、graph.numEdges  返回当前图的边的数量
2、graph.numVertices  返回当前图的顶点的数量
3、graph.inDegrees    返回当前图每个顶点入度的数量，返回类型为VertexRDD[Int]
4、graph.outDegrees   返回当前图每个顶点出度的数量，返回的类型为VertexRDD[Int]
5、graph.degrees      返回当前图每个顶点入度和出度的和，返回的类型为VertexRDD[Int]



1、`def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2) (implicit eq: VD =:= VD2 = null): Graph[VD2, ED]`
  对当前图每一个顶点应用提供的map函数来修改顶点的属性，返回一个新的图。
2、`def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): Graph[VD, ED2]`  对当前图每一条边应用提供的map函数来修改边的属性，返回一个新图。
3、`def mapTriplets[ED2: ClassTag](map: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2] ` 对当前图每一个三元组应用提供的map函数来修改边的属性，返回一个新图。

### 应用



















