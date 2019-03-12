[TOC]



## spark集群

### 1 配置文件

#### 1 slaves

这个可以把spark 的节点都写上, 写上后, 启动的时候, 根据文件启动worker 进程

#### 2 spark-env.sh

SPARK_MASTER_PORT=7077

export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=4000
-Dspark.history.retainedApplications=3
-Dspark.history.fs.logDirectory=hdfs://master01:9000/directory"

export SPARK_DAEMON_JAVA_OPTS="
-Dspark.deploy.recoveryMode=ZOOKEEPER
-Dspark.deploy.zookeeper.url=zk1,zk2,zk3
-Dspark.deploy.zookeeper.dir=/spark"

#### 3 spark-default.conf

spark.eventLog.enabled true
spark.eventLog.dir hdfs://master01:9000/directory
spark.eventLog.compress true

### 2 启动

1 在 hbase-1 上启动 sbin/start-all.sh 然后在hbase-2 上启动 sbin/start-master.sh

### web 在8080 

### 例子程序

1. #### job 算圆周率

```shell
spark-submit --class org.apache.spark.examples.SparkPi \ 
--master spark://spark-1:7077  \
--executor-memory 1G  \
--total-executor-cores 2    \
/usr/local/spark-2.4.0-bin-hadoop2.7/examples/jars/spark-examples_2.11-2.4.0.jar


```

#### spark-shell wordcount

```shell
sc.textFile("hdfs://hadoop-1:9000/hqltest.hql").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("hdfs://hadoop-1:9000/ourt")

```



`java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=20000 -Xmx1g --jar  spark-core_2.11-2.4.0.jar  org.apache.spark.deploy.SparkSubmit`

### 应用提交

```
spark-submit
--class
--master master-URL (spark://host:port)
--deploy-mode   client cluster
--conf
application-jar
application-arguments

```

`--master` : 

`local[k] ` : 本地以k worker 线程

` local[*]  ` : 本机以本机同样核数的线程运行

`spark://host:port`  提交到mster 上

`  mesos://host:port `  

`   yarn-client `    以client 模式连接到yarn cluster

`  yarn-cluster`   以cluster 模式连接到yarn cluster

比如有1个master, 3个worker, 一个idea写了spark 程序, 一般情况下我们把spark 打包放在master 运行, 默认是client, 在master 机器上新建一个jvm , 称为driver, 向master申请资源, worker会生成executor(jvm), executor 运行程序, 如果是cluster, 会在worker里面随便找一台机器启动executor, driver 运行在executor, driver 负责在计算过程中调度, 分配任务, 回收结果.

### 

## scala 

#### 协变

```scala
class Person
class Asia extends Person
class Chinese extends Asia
val asia:Asia = new Asia
vals person:Person = asia
val chinese:Chinese = asia
class Group[T](t:T)
val groupAsia:Group[Asia] = new Group(new Asia)
val groupAsia:Group[Person]=groupAsia  //错误
class Group[+T](t :T)
class Group1[+T](t :T)
val groupAsia1:Group[Asia]=new Group[Asia](new Asia)
val groupPerson1:Group[Person] = groupAsia1  
val chineseGroup1:Group[Chinese] = new Group(new Chinese)
val asiaGroup:Group[Asia] = chineseGroup1
val personGroup:Group[Person] = chineseGroup1
```

#### 逆变

```scala
class Group2[-T](t :T)
val asiaGroup2:Group2[Aisa] = new Group2[Aisa](new Asia)
val chineseGroup2:Group2[Chinese] = asiaGroup2
```

#### 上界



```scala
class Group3[T <:Asia](t:T)
val chineseGroup3:Group3[Chinese] = new Group3[Chinese](new Chinese)
val asia: Group3[Asia] = chineseGroup3   //错误, 已经没有协变的定义了
val personGroup3:Group3[Person] = new Group3[Person](new Person) // 不符合上界定义
```

下界

```scala
class Group4[T>:Asia](t: T)
val personGroup4:Group4[Person] = new Group4[Person](new Person)
val chineseGroup4:Group4[Chinese] = new Group4[Chinese](new Person) //错误
val chineseGroup4:Group4[Chinese] = new Group4[Chinese](new Chinese)// 错误
val chineseGroup4 = new Group4(new Chinese) //可以, 强制转换为Asia 了 
```

里氏替换原则

```scala
class Group5[+T](t: T){
    def f1[T](t: T) = {}
}
class Group6[+T](t:T){   // 错误
    def f2[S <:T](s: S) = {}  // S 是 T 的子类, 子类有更多的束缚条件
}
class Group7[+T](t:T){
    def f3[S>:t](s : S) = {}  // 可以有一个t 的父类S 传进来
}
```



```scala
class Group5[+T](t : T){
    def f1[T](t : T)={}
    // def f2[S <:T](s: S) = {}  // 错误
    def f3[Z>:T](z : Z)={}
}

class Group6[-T](t : T){
    def f1[T](t:T)={}
    def f2[S <:T](s : S) = {}
    // def f3[Z >:T](z: Z) = {}   // 错误
}
```



#### idea 中写spark

```scala
import org.apache.spark.{SparkConf, SparkContext}
object WordCount extends App{
  val sparkConf = new SparkConf().setAppName("word").setMaster("local[*]")
  val sparkcontext = new SparkContext(sparkConf)
  val file = sparkcontext.textFile("hdfs://hadoop-1:9000/hqltest.hql")

  val words = file.flatMap(_.split(" "))
  val wordsTuple = words.map((_,1))
  wordsTuple.reduceByKey(_+_)saveAsTextFile("hdfs://hadoop-1:9000/out2")
  sparkcontext.stop()


}
```

#### hdfs 写入权限问题

* hdfs-site.xml 中配置dfs.permissions false
* hadoop fs -chmod 777 /
* 在idea 中  edit configuration,   HADOOP_USER_NAME root

`java -cp /usr/local/spark-2.4.0-bin-hadoop2.7/jars/* -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=20000 -Xmx1g org.apache.spark.deploy.SparkSubmit`

#### idea 提交到远程集群

```scala
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
```



## RDD

### 概念

RDD 是整个spark 计算的基石, 是分布式数据的抽象, 为用户屏蔽了底层复杂计算和映射环境

* Rdd 是不可变的, 如果在一个RDD 上进行转换操作, 会生成一个新的RDD
* Rdd 是分区的, RDD 里面的具体数据是分布在多台机器上的Executor 里面的, 堆内存和堆外内存 + 硬盘
* Rdd 是弹性的

#### 弹性表现

1. 存储的弹性: 内存和磁盘的自动切换
2. 容错的弹性, 数据丢失可以自动恢复
3. 计算的弹性: 计算出错重试机制
4. 分片的弹性: 根据需要重新分片

`sc.txtFile("xx").flatMap(_.split(" ")).map((_.1)).reduceByKey(_+_).saveAsTextFile("xx")`

RDD 创建  转换  缓存  行动  输出

### 编程

#### 创建

* 用Scala 集合里面创建  1. sc.parallelize(seq) 把seq 这个数据并行化分片到节点
* sc.makeRDD(seq) 把seq 这个数据并行化分片到节点, 它的实现就是parallelize
* sc.makeRDD(seq(T, seq)) 这种方式可以指定RDD 存放位置

2 从外部存储来创建  比如  sc.textFile("path")

3 从另外一个RDD 转换 过来



RDD 所有转换操作都是懒执行的, 只有当行动操作出现的时候spark 才会真正运行

假如一个rdd 有6个数据, 两个partition, 给一个rdd flatMap, 那么函数会被调用6次, 给一个mappartitions() 会被调用2次

mappartitions 将函数应用于RDD 的每个分区, 每一个分区运行一次, 函数需要能接受Iterator 类型, 然后返回Iterator

mapPartitionsWithIndex

#### 转换操作

* ` map[U: ClassTag](f: T => U): RDD[U] ` 将函数应用于RDD 每一个元素, 并返回一个新的RDD

  ```java
  var t1 = sourceRdd.map(_*2)
  ```

* ` filter(f: T => Boolean): RDD[T] ` 通过提供的产生boolean 条件的表达式来返回符合条件结果为true 的新的RDD

  ```
  var filter = sc.makeRDD(Array("aa1","bb1","cc1"))
  filter.filter(_.startsWith("aa")).collect()
  ```

* `flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U]` 将函数应用于RDD 中每一项, 对于每一项都产生一个集合, 并将集合中的元素压扁成一个集合

  ```
  var flat = sc.makeRDD(1 to 3)
  flat.flatMap((1 to _)).collect()
  ```

* `mapPartitions[U: ClassTag]( f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = **false**): RDD[U] ` 将函数应用于RDD 的每一个分区, 一个分区运行一次, 函数需要能够接受Iterator 类型, 然后返回Iterator.

  ```
  var person = sc.makeRDD(List(("a","female"),("b","male"),("c","female")))
  def partitionsFun(iter: Iterator[(String, String)]) : Iterator[String]={
    var woman = List[String]()
    while(iter.hasNext){
      var next = iter.next()
      next match {
        case (_,"female") => woman = next._1::woman
        case _=>
      }
    }
    woman.iterator
  }
  person.mapPartitions(partitionsFun).collect()
  ```

* `mapPartitionsWithIndex[U: ClassTag]( f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = **false**): RDD[U] `  将函数应用于RDD 中每一个分区, 每一个分区运行一次, 函数能够接受一个分区的索引值, 和一个代表分区内所有数据的iterator,需要返回iterator

  ```
  def partitionsFun(index : Int, iter: Iterator[(String,String)]):Iterator[String]={
    var woman = List[String]()
    while(iter.hasNext){
      var next = iter.next()
      next match{
        case (name, "female") => woman = "["+index.toString+"]"+name::woman
        case _=>
      }
    }
    woman.iterator
  }
  
  person.mapPartitionsWithIndex(partitionsFun).collect()
  person.partitions.size
  ```

* `sample(withReplacement: Boolean, fraction: Double, seed: Long = Utils.*random*.nextLong): RDD[T]`   以seed 为种子,返回大致上有fraction比例个数据样本RDD, withReplacement 表示是否采用放回式抽样

  ```
  var sample = sc.makeRDD(1 to 100)
  sample.sample(false, 0.1 ,4).collect
  sample.sample(true, 0.1, 4).collect
  sample.takeSample(false,20,4)
  ```

* `union(other: RDD[T]): RDD[T]` 将两个RDD 中的元素进行合并, 返回一个新的RDD

  ```
  var a = sc.makeRDD(1 to 10)
  sc.makeRDD(5 to 15).union(a).collect
  ```

* `intersection(other: RDD[T]): RDD[T]` 将两个RDD 做交集, 返回一个新的RDD

  ```
  var aa = sc.makeRDD(1 to 10)
  var bb = sc.makeRDD(5 to 15)
  aa.intersection(bb).collect
  ```

* `distinct(): RDD[T]`  将当前RDD进行去重后,返回一个新的RDD

  ```
  var dis = sc.makeRDD(List(1,2,1,2,3,4,2))
  dis.distinct.collect
  ```

* `partitionBy(partitioner: Partitioner): RDD[(K, V)]` : 将设置的分区器重新将RDD 进行分区, 返回新的RDD

  ```
  rdd1.partitions.size
  var rdd2 = rdd1.partitionBy(new org.apache.spark.HashPartitioner(2))
  rdd2.partitions.size
  ```

* `reduceByKey(func: (V, V) => V): RDD[(K, V)] `  根据key 值将相同key 的元组的值用func 进行计算, 返回新的RDD

  ```
  var reduce = sc.makeRDD(List(("f",2),("f",4),("m",5),("m",5)))
  
  reduce.reduceByKey(_+_).collect
  ```

* `groupByKey(): RDD[(K, Iterable[V])]` 将相同的key 的值进行聚集, 输出一个 (K, iterator)类型的RDD

  ```
  var group = sc.makeRDD(List("a","b","c","a","b"))
  var g = group.map((_,1)).groupByKey()
  g.map(x=>(x._1,x._2.sum)).collect
  ```

* `combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, numPartitions: Int): RDD[(K, C)] ` createCombiner, mergeValue, mergeCombiners numPartitions, 根据key 分别使用createCombiner 和mergeValue 进行相同key 的数值聚集, 通过mergeCombiners 将各个分区最终结果进行聚集

  ```
  var scores = Array(("Fred", 95),("Fred", 95), ("Fred",91),("Wilma",93),("Wilma",95),("Wilma",98))
  var input = sc.parallelize(scores)
  var combine = input.combineByKey((v) => (v,1), (acc:(Int,Int),v) =>(acc._1,acc._2+1),(acc1:(Int,Int), acc2:(Int,Int))=>(acc1._1+acc2._1, acc1._2+acc2._2))
  ```

* `aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,combOp: (U, U) => U): RDD[(K, U)] ` 按照key 将value 进行分组合并,将每个value 和 (seqOp 用于在每个分区中用初始值迭代value, combOp 函数用于合并每个分区中的结果 ), 

  ```
  var rr = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
  var rr1 = rr.aggregateByKey(0)(math.max(_,_),_+_)
  ```

* `foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)]` appregateByKey 简化操作, seqop 和combop 相同

  ```
  var rdd3 = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
  val rd3 = rdd3.foldByKey(0)(_+_)
  rd3.collect()
  ```

* `sortByKey(ascending: Boolean = **true**, numPartitions: Int = self.partitions.length): RDD[(K, V)]` 在一个(K,v) 的RDD 上调用, K 必须实现ordered 借口, 返回一个按照key 进行排序的(k,v) 的rdd

  ```
  val rdd = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
  rdd.sortByKey(true).collect() //升序
  rdd.sortByKey(false).collect()
  ```

* `sortBy[K](f: (T) => K,ascending: Boolean = **true**,numPartitions: Int = **this**.partitions.length)(**implicit** ord: Ordering[K], ctag: ClassTag[K]): RDD[T]`底层还是使用sortbykey, 只不过使用fun 生成的新的key进行排序

  ```
  val rdd = sc.parallelize(List(1,2,3,4))
  rdd.sortBy(x => x).collect()
  rdd.sortBy(x => x%3).collect()
  ```

* `join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))]`在类型(k, v)和(k,w) 的rdd 上调用, 返回一个相同key对应的素有元素对在一起的(k, (v, w)) 的rdd,   它只会返回key 在两个rdd 中都存在的情况 

* `cogroup[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Iterable[V], Iterable[W]))]` 在(k, v) 和(k, w) 的rdd 上调用, 返回一个(k, (iterable<V>, iterable<W>))类型的rdd, 

  ```
  val a = sc.makeRDD(List((1,"a"),(2,"b"),(3,"c")))
  val b = sc.makeRDD(List((1,1),(2,2),(4,"b")))
  a.join(b).collect()
  a.cogroup(b).collect()
  val c = sc.makeRDD(List((1,1),(2,"C"),(4,"c")))
  a.cogroup(c)
  ```

* `cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)]` 做两个rdd 的笛卡尔积, 返回对偶的rdd

  ```
  val rdd1 = sc.parallelize(1 to 3)
  val rdd2 = sc.parallelize(2 to 5)
  rdd1.cartesian(rdd2).collect()
  ```

* `pipe(command: String): RDD[String]` 对于每个分区, 都执行一个perl 或者shell 脚本, 返回输出的rdd, 

  ```
   /*
    #!/bin/sh
    echo "A"
    while read line ; do
    echo ">>>" ${line}
    done
     */
  val rdd = sc.parallelize(Array("HOW","ARE","YOU"))
    rdd.pipe("/home/bigdata/pipe.sh").collect()
     rdd.partitions.size
    rdd.mapPartitionsWithIndex((index, iter) => Iterator(index.toString + ":"+iter.mkString("|"))).collect()
  ```

* `coalesce(numPartitions: Int, shuffle: Boolean = **false**,partitionCoalescer: Option[PartitionCoalescer] = Option.*empty*)(implicit ord: Ordering[T] = null): RDD[T]`: 缩减分区数, 用于大数据过滤后, 提高下数据集的执行效率

  ```
  val rdd = sc.parallelize(1 to 16 , 4)
  rdd.mapPartitionsWithIndex((index, iter)=> Iterator(index.toString +":"+iter.mkString("|"))).collect()
  val rdd2 = rdd.coalesce(3)
  rdd2.mapPartitionsWithIndex((index, iter)=> Iterator(index.toString +":"+iter.mkString("|"))).collect()
  ```

* `repartition(numPartitions: Int)(**implicit** ord: Ordering[T] = **null**): RDD[T]` 根据传入的分区数重新通过网络分区所有数据

  ```
  val rdd = sc.parallelize(1 to 16, 4)
  rdd.partitions.size
  val rerdd = rdd.repartition(2)
  rerdd.partitions.size
  rdd.repartition(4).collect()
  ```

* `repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)]` 性能要比repartition 要高, 在给定的partitioner 中排序

* `glom(): RDD[Array[T]]`将每一个分区形成一个数组, 形成新的rdd 类型rdd [Array[T]]

  ```
  val glom = sc.makeRDD(1 to 10 , 4)
  glom.mapPartitionsWithIndex((index, iter)=>Iterator(index.toString +":"+iter.mkString("|"))).collect()
  val aa = glom.glom()
  aa.collect()
  ```

* `mapValues[U](f: V => U): RDD[(K, U)]` 将函数应用于 (k v), 结果中的 v 返回新的rdd

  ```
  val rdd3 = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))
  rdd3.mapValues(_+"|||").collect()
  ```

* `subtract(other: RDD[T]): RDD[T]` 计算差的一种函数, 去除两个rdd 中相同的元素, 不同的rdd 将保留下来

  ```
  val rdd = sc.parallelize(3 to 8)
  val rdd1 = sc.parallelize( 1 to 5)
  rdd.subtract(rdd1).collect()
  ```

* 

#### 行动操作

* takeSample 抽样但是返回一个scala 集合

* `reduce(f: (T, T) => T): T`  通过func 函数聚集rdd 中所有元素

  ```
  val rdd1 = sc.makeRDD(1 to 10 ,5)
  rdd1.reduce(_+_)
  ```

* `collect(): Array[T]` 以数组的形式返回数据集的所有元素

* `count(): Long` 返回rdd 中元素的个数

* `first(): T ` 返回rdd 中第一个元素

* `take(num: Int): Array[T]` 返回rdd 中前n 个元素

* `takeOrdered(num: Int)(**implicit** ord: Ordering[T])` 返回前几个的排序

* ` aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U` aggregate 将每个分区里面的元素通过seqop 和初始值进行聚合, 然后用combine 函数将每个分区的结果和初始值zeroValue 进行combine 操作, 这个函数最终返回的类型不需要和rdd 中元素类型一致

* `fold(zeroValue: T)(op: (T, T) => T): T` 折叠操作, aggregate 的简化操作

  ```
  val rdd = sc.makeRDD(1 to 4,2)
  rdd.aggregate(1)((x,y) => x+y,(a,b)=>a + b)
  rdd.fold(1)(_+_)
  ```

* `saveAsTextFile(path: String): Unit` 以文本文件方式保存到本地或者hdfs

* `saveAsObjectFile(path: String): Unit` 序列化后保存

* `countByKey(): Map[K, Long]`对于(k v) 类型的rdd, 返回一个(k , int) 的map, 表示每个key 对应的元素个数

  ```
  val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
  rdd.countByKey()
  ```

* `foreach(f: T => Unit): Unit`

当在RDD 中使用到了class 的方法或属性的时候, class 需要继承java.io.Serializable 接口, 乎哟这可以将属性赋值为本地变量来防止整个对象的传输

```scala
class SearchFunctions(val query: String) extends java.io.Serializable{
    def isMatch(s ;String): Boolean = {
        s.contains(query)
    }
    def  getMatchesFunctionReference(rdd: org.apache.spark,rdd,RDD[String]): org.apache.spark.rdd.RDD[String]={
        rdd.filter(isMatch)
    }
    def getMatchesFieldReference(rdd: org.apache.spark.rdd.RDD[String]):
    org.apache.spark.rdd.RDD[String] = {
        rdd.filter(x => x.contains(query))
    }
    def getMatchesNoReference(rdd: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[String] = {
        val query_ = this.query
        rdd.filter(x => x.contains(query_))
    }
}
```



### RDD 依赖关系

窄依赖: 每一个父RDD 的partition 最多被一个子RDD 的一个partition 使用, 出度为1

宽依赖: 多个子RDD 的partition 会依赖同一个父RDD 的partition, 会引起shuffle, 出度大于等于2

应用在整个过程中, RDD 之间形成的产生关系, 称为血统关系, RDD 在没有持久化的时候默认是不保存的, 如果需要那么就根据血统关系重新计算

应用执行过程中, 分为多个stage 来进行的, 划分stage 的关键就是判断是不是存在宽依赖, 

#### rdd 持久化

* rdd 持久化主要通过persist, cache 操作来实现, cache 操作相当于stoageLevel1 为memory_only 的persist 操作
* 持久化它的存储等级可以分为, 存储的位置(磁盘, 内存, 非堆内存), 是否序列化, 存储的份数

#### rdd 检查点

checkpoint 是把rdd 存放在hdfs 中, 是多副本可靠存储的, 

```
val check = sc.makeRDD(1 to 10)
sc.setCheckpointDir("hdfs://hadoop-1:9000/checkpoint")
check.ckeckpoint
check.isCheckpointed
check.collect
check.isCheckpointed

val check2 = check.map(_.toString+"[" + System.currentTimeMillis + "]")
val nocheck = check.map(_.toString + "[" + System.currentTimeMillis + "]")
check2.checkpoint
nocheck.collect
nocheck.collect
check2.collect
check2.collect
```

1. 创建一个RDD
2. 需要设置sparkContext, 他的checkpoint, 
3. 在rdd 调用checkpoint 方法
4. 出发rdd 行动操作, 让 rdd 真是数据写入checkpoint目录

#### 分区

```scala
val intRdd = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c"),(4,"d")),8)
intRdd.collect
intRdd.mapPartitionsWithIndex((index, iter) => Iterator("[]"+index.toString+"]:[" +iter.mkString(",")+"]")).collect
intRdd.partitioner
val rdd1 = intRdd.partitionBy(new org.apache.spark.HashPartitioner(3)) // 重新分3 个分区
rdd1.partitions.size
rdd1.mapPartitionsWithIndex((index, iter) => Iterator("[]"+index.toString+"]:[" +iter.mkString(",")+"]")).collect
```

#### 自己创建partitioner

```scala
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
/**
* Created by wuyufei on 04/09/2017.
*/
class CustomerPartitioner(numParts:Int) extends Partitioner {
//覆盖分区数
override def numPartitions: Int = numParts
//覆盖分区号获取函数
override def getPartition(key: Any): Int = {
val ckey: String = key.toString
ckey.substring(ckey.length-1).toInt%numParts
}
}
object CustomerPartitioner {
def main(args: Array[String]) {
val conf=new SparkConf().setAppName("partitioner")
val sc=new SparkContext(conf)
val data=sc.parallelize( List ("aa.2","bb.2","cc.3","dd.3","ee.5"))
data.map((_,1)).partitionBy(new
CustomerPartitioner(5)).keys.saveAsTextFile("hdfs://master01:9000/partitioner"
)
}
}
```

#### RDD 累加器 和广播变量

```
val notice = sc.textFile("./NOTICE")
val blanklines = sc.accumulator(0)
val tmp = notice.flatMap(line =>{
    if(line == "")
    blanklines +=1
})
tmp.collect
blanklines.value
```

* 需要通过sparkContext 声明一个累加器 方法名字  accumulator(), 提供累加器的初始值
* 你可以在转换操作或者行动上直接使用累加器, 可以通过 += 操作符 增加累加器的值, 但是不能够读取累加器. 一般, 不推荐在转换操作使用累加器, 一般在**行动操作**中使用
* driver 可以通过累加器.value  来读取累加器的值, 并输出

#### 自定义累加器

继承  AccumulatorV2,  然后提供类型参数1 你的增加值类型参数, 2 输出值类型参数

```scala
class LogAccumulator extends AccumulatorV2[String, java.util.Set[String]] {
    private val _logArray : java.util.Set[String] = new
    java.util.HashSet[String]()
    override def isZero: Boolean = {
        _logArray .isEmpty
    }
    override def reset(): Unit = {
        _logArray .clear()
    }
    override def add(v: String): Unit = {
        _logArray .add(v)
    }
    override def merge(other: AccumulatorV2[String, java.util.Set[String]]): Unit= {
        other match {
            case o: LogAccumulator => _logArray .addAll(o.value)
        }
    }
    override def value: java.util.Set[String] = {
        java.util.Collections. unmodifiableSet ( _logArray )
    }
    override def copy(): AccumulatorV2[String, java.util.Set[String]] = {
        val newAcc = new LogAccumulator()
        _logArray .synchronized{
            newAcc. _logArray .addAll( _logArray )
        }
        newAcc
    }
}

object LogAccumulator {
    def main(args: Array[String]) {
        val conf=new SparkConf().setAppName("LogAccumulator")
        val sc=new SparkContext(conf)
        val accum = new LogAccumulator
        sc.register(accum, "logAccum")
        val sum = sc.parallelize( Array ("1", "2a", "3", "4b", "5", "6", "7cd", "8",
                                         "9"), 2).filter(line => {
            val pattern = """^-?(\d+)"""
            val flag = line.matches(pattern)
            if (!flag) {
                accum.add(line)
            }
            flag
        }).map(_.toInt).reduce(_ + _)
        println ("sum: " + sum)
        println ("sum: " + accum.value.size)
        for (v <- accum.value) print (v + " >>>>>>>>>>>>>>>>> ")
        println ()
        sc.stop()
    }
}
```

创建一个sparkcontext, 创建一个自定义累加器, 注册累加器  sc.register(accum, "logAccum")

#### 广播变量

如果算法中使用了广播变量, spark 会把闭包中所有引用道德变量分发到所有工作节点中, 很低效. 如果使用广播变量, 那么会把变量分发到每一个worker, 会提高整个的效率

```
val broadcaseVar = sc.broadcast(Array(1,2,3))
broadcaseVar.value
```

1. 通过对一个类型T的对象调用SparkCOntext.broadcase 创建一个Broadcase 对象, 任务可序列化类型都可以这么实现
2. 通过value 属性访问该对象的值, 
3. 对象只会发到各个节点一次, 应作为只读值处理

## 数据读取与保存主要方式

hdfs 上面文件的block 数量和 rdd 的partition 数一般是相同的,HDFS中的文件block数为1，那么Spark设定了最小的读取partition数为2

#### 文本文件

```
sc.textFile("./README")
val readme = sc.textFile("./README")  //本地文件系統
readme.collect()
readme.saveAsTextFile("hdfs://hadoop-1:9000/test")  // hdfs 文件系統
```

#### json

还是通过文本文件的输入和输出, spark core 中没有对json 和csv 文件的解析和反解析工具, 

```scala
object test extends App{
    import org.json4s
    import org.json4s.jackson.JsonMethods._
    import org.json4s.jackson.Serialization
    var result = sc.textFile("examples/src/main/resources/people.json")
    class Person(var name: String,var age: Int)
    implicit val formats = Serialication.formats(ShortTypeHints(List()))
    result.collect().foreach(x => {var c = parse(x).extract[Person]; println(c.name + "," + c.age)})
}
```

如果json 是一行一个json , 那么比较好解决

如果不是一行一个, 那么需要用json 包整体解析

#### csv

把csv 当做普文本文件读取数据, 通过将每一行进行解析实现对csv 的读取, 

#### sequenceFile

他是hadoop 用来存储二进制形式的key -value 对而设计的一种平面文件, 调用saveAsHadoopFIle

```scala
object test extends App{
    val data = sc.parallelize(List((2, "aa"),(3,"bb"),(4,"cc"),(5,"dd")))
    data.saveAsSequenceFile("hdfs://hadoop-1:9000/seqFile")
    val sdata = sc.sequenceFile[Int, String]("hdfs://hadoop-1:9000/seqFile")
    sdata.collect()
}
```

#### objectFile

调用了sequenceFIle,

```scala
object test extends App{
    val data = sc.parallelize(List((2, "aa"), (3,"bb"),(4,"cc"),(5,"dd"),(6,ee)))
    data.saveAsObjectFile("hdfs://hadoop-1:9000/objfile")
    import org.apache.spark.rdd.RDD
    val objrdd:RDD[(Int, String)] = sc.objectFile[(Int, String)]("hdfs://hadoop-1:9000/objfile")
    objrdd.collect
}
```

对于ObjectFile他的读取和保存使用了读取和保存SequenceFile的API，也最终调用了hadoop的API。

#### hadoop 的输入输出格式

整个spark 生态系统和hadoop 是完全兼容的.hadoop 的api 有新旧两套版本, hadoopRDD 和 newHadoopRDD 是两个函数借口, 主要包含一下四个参数

1. 输入格式, 制定数据输入的类型, 如TextInputFormat, 新旧两个版本使用的是org.apache.hadoop.mapred.InputFormat  和  org.apache.hadoop.mapreduce.InputFormat
2. 键类型
3. 值类型
4. 分区值

读取: newApiHadoopFIle 和 newApiHadoopRDD 两个方法, 最终都是调用newApiHadoopRDD来进行实现

输出: saveAsNewApiHadoopFile 和saveAsNewApiHadoopDataset 两个方法, 最终都是调用 saveAsNewApiHadoopDataset这个方法进行实现

比旧的hadoopApi 主要进行可以压缩存储文件到hdfs 进行了优化

### 数据库

#### mysql

读取

```scala
def main(args: Array[String]){
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("mysql")
    val rdd = new JdbcRDD(
        sc, 
        () => {
            Class.forName("com.mysql.jdbc.Driver").newInstance()
            DriverManager.getConnection("jdbc:mysql://linux01:3306/company", "root","123456")

        },
        "select * from staff where id >= ? and id <= ?;"
        1,
        100,
        1,
        r=>(r.getString(1), r.getString(2),r.getString(3)).cache()
        println(rdd.count())
        rdd.foreach(println(_))
    )
    sc.stop()
}
```

#### 写入

```
def main(args: Array[String]){
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("mysql")
    val sc = new SparkContext(sparkConf)
    val data = sc.parallelize(List(("Irelia","Female"),("Ezreal","Male"),("qing","Female"),("lei","mALE")))
    data.foreachPartition(insertData)
}

def insertData(iterator: Iterator[(String, String)]): Unit = {
    val conn = DriverManager.getConnection("jdbc:mysql://linux01:3306/company",
"root", "123456")
iterator.foreach(data =>{
    val ps = conn.prepareStatement("insert into staff(name, sex) values (?,?)")
    ps.setString(1, data._1)
    ps.setString(2, data._2)
    ps.executeUpdate()
})
}
```

### hbase 

通过org.apache.hadoop.hbase.mapreduce.TableInputFormat 类实现

#### 读取

```scala
def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HBaseApp")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    //HBase 中的表名
    conf.set(TableInputFormat.INPUT_TABLE, "fruit")
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
                                      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                                      classOf[org.apache.hadoop.hbase.client.Result])
    val count = hBaseRDD.count()
    println("hBaseRDD RDD Count:"+ count)
    hBaseRDD.cache()
    hBaseRDD.foreach {
        case (_, result) =>
        val key = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue("info".getBytes,
                                                  "name".getBytes))
        val color = Bytes.toString(result.getValue("info".getBytes,
                                                   "color".getBytes))
        println("Row key:" + key + " Name:" + name + " Color:" + color)
    }
    sc.stop()
}
```

#### 写入

```scala
def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HBaseApp")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat( classOf [TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "fruit_spark")
    val fruitTable = TableName.valueOf("fruit_spark")
    val tableDescr = new HTableDescriptor(fruitTable)
    tableDescr.addFamily(new HColumnDescriptor("info".getBytes))
    val admin = new HBaseAdmin(conf)
    if (admin.tableExists(fruitTable)) {
        admin.disableTable(fruitTable)
        admin.deleteTable(fruitTable)
    }
    admin.createTable(tableDescr)
    def convert(triple: (Int, String, Int)) = {
        val put = new Put(Bytes.toBytes(triple._1))
        put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("name"),
                         Bytes.toBytes(triple._2))
        put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("price"),
                         Bytes.toBytes(triple._3))
        (new ImmutableBytesWritable, put)
    }
    val initialRDD = sc.parallelize(List((1,"apple",11), (2,"banana",12),
                                         (3,"pear",13)))
    val localData = initialRDD.map(convert)
    localData.saveAsHadoopDataset(jobConf)
}
```





























































