import org.apache.spark.{SparkConf, SparkContext}

object RDDTRY extends App {
  var sparkConf = new SparkConf().setAppName("word").setMaster("local[*]")
  var sc = new SparkContext(sparkConf)
  var sourceRdd = sc.makeRDD(1 to 10)
  var t1 = sourceRdd.map(_*2)
  t1.collect() //执行了collect, 才会真正执行
  var filter = sc.makeRDD(Array("aa1","bb1","cc1"))
  filter.filter(_.startsWith("aa")).collect()

//  var ff = sc.textFile("abc.txt") // 不会立即执行
//  ff.collect()  //报错

  var flat = sc.makeRDD(1 to 3)
  flat.flatMap((1 to _)).collect()

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

  var sample = sc.makeRDD(1 to 100)
  sample.sample(false, 0.1 ,4).collect
  sample.sample(true, 0.1, 4).collect
  sample.takeSample(false,20,4)

  var a = sc.makeRDD(1 to 10)
  sc.makeRDD(5 to 15).union(a).collect

  var aa = sc.makeRDD(1 to 10)
  var bb = sc.makeRDD(5 to 15)
  aa.intersection(bb).collect

  var dis = sc.makeRDD(List(1,2,1,2,3,4,2))
  dis.distinct.collect

  val rdd1 = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)

  rdd1.partitions.size
  var rdd2 = rdd1.partitionBy(new org.apache.spark.HashPartitioner(2))
  rdd2.partitions.size

  var reduce = sc.makeRDD(List(("f",2),("f",4),("m",5),("m",5)))

  reduce.reduceByKey(_+_).collect

  var group = sc.makeRDD(List("a","b","c","a","b"))
  var g = group.map((_,1)).groupByKey()
  g.map(x=>(x._1,x._2.sum)).collect

  var scores = Array(("Fred", 95),("Fred", 95), ("Fred",91),("Wilma",93),("Wilma",95),("Wilma",98))
  var input = sc.parallelize(scores)
  var combine = input.combineByKey((v) => (v,1), (acc:(Int,Int),v) =>(acc._1,acc._2+1),(acc1:(Int,Int), acc2:(Int,Int))=>(acc1._1+acc2._1, acc1._2+acc2._2))

 var result = combine.map{case (key, varue) =>(key, varue._1 / varue._2)}

  // (1,3)  (1,4)(2,3) (3.8)      ==>(1,7) (2,3)(3,8)
  var rr = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
  var rr1 = rr.aggregateByKey(0)(math.max(_,_),_+_)

  var rdd3 = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
  val rd3 = rdd3.foldByKey(0)(_+_)
  rd3.collect()

  {
    val rdd = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    rdd.sortByKey(true).collect() //升序
    rdd.sortByKey(false).collect()

  }
  {
    val rdd = sc.parallelize(List(1,2,3,4))
    rdd.sortBy(x => x).collect()
    rdd.sortBy(x => x%3).collect()
  }

  {
    val a = sc.makeRDD(List((1,"a"),(2,"b"),(3,"c")))
    val b = sc.makeRDD(List((1,1),(2,2),(4,"b")))
    a.join(b).collect()
    a.cogroup(b).collect()
    val c = sc.makeRDD(List((1,1),(2,"C"),(4,"c")))
    a.cogroup(c)
  }

  {
    val rdd1 = sc.parallelize(1 to 3)
    val rdd2 = sc.parallelize(2 to 5)
    rdd1.cartesian(rdd2).collect()
  }
  {
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
}
  {
    val rdd = sc.parallelize(1 to 16 , 4)
    rdd.mapPartitionsWithIndex((index, iter)=> Iterator(index.toString +":"+iter.mkString("|"))).collect()
    val rdd2 = rdd.coalesce(3)
    rdd2.mapPartitionsWithIndex((index, iter)=> Iterator(index.toString +":"+iter.mkString("|"))).collect()

  }

  {
    val rdd = sc.parallelize(1 to 16, 4)
    rdd.partitions.size
    val rerdd = rdd.repartition(2)
    rerdd.partitions.size
    rdd.repartition(4).collect()
  }
  {
    val glom = sc.makeRDD(1 to 10 , 4)
    glom.mapPartitionsWithIndex((index, iter)=>Iterator(index.toString +":"+iter.mkString("|"))).collect()
    val aa = glom.glom()
    aa.collect()
  }

  {
    val rdd3 = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))
    rdd3.mapValues(_+"|||").collect()
  }
  {
    val rdd = sc.parallelize(3 to 8)
    val rdd1 = sc.parallelize( 1 to 5)
    rdd.subtract(rdd1).collect()
  }


  //action
  {
    val rdd1 = sc.makeRDD(1 to 10 ,5)
    rdd1.reduce(_+_)
  }
  {
    val rdd = sc.makeRDD(1 to 4,2)
    rdd.aggregate(1)((x,y) => x+y,(a,b)=>a + b)
    rdd.fold(1)(_+_)
  }
  {
    val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    rdd.countByKey()

  }

}
