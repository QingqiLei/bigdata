[TOC]



### 介绍

#### 概念

spark sql 是spark 套件中的一个模块, 他将数据的计算任务通过sql 的形式转换成了RDD 计算, 类似hive 通过sql 的形式将数据的计算任务转换成mapreduce

#### 特点

1. 和spark core 无缝集成, 我可以在写整个RDD 应用的时候, 配置spark sql 来实现我的逻辑
2. 统一的数据访问方式, spark sql 提供标准化的sql 查询
3. hive 的继承, spark sql 通过内嵌hive 或者连接外部已经部署好的hive 实例, 实现对hive 语法的继承和操作
4. 标准化的连接方式, spark sql 可以通过启动thrift server 来支持jdbc, odbc 的访问, 将自己作为一个BI server 来使用

#### sql sql 数据抽象

users.join(events, users("id") === events("uid")).filter(events("date") > "2015-01-01")  优化: 先filter再join.

1. RDD (spark 1.0)  => DataFrame (Spark1.3) => DataSet(Spark1.6)
2. DataFrame 就是RDD + schema, 可以认为是一张二维表格, 他的劣势是编译器不进行表格中字段的**类型检查**, 在运行期检查. (懒执行, DataFrame 存放在 堆外, 不在jvm 中) (编译期没有类型检查)
3. DataSet 是Spark 最新的数据抽象, Spark 的发展会逐步将DataSet 作为主要的数据抽象, 弱化RDD 和DataFrame, DataSet 包含了DataFrame所有的优化机制, 此外提供了以样例类为schema 模型的强类型
4. DataFrame = DataSet[Row]   dataframe 是由row 组成的
5. DataFrame 和DataSet 都有可控的内存管理机制, 所有数据都保存在非堆上, 都使用catalyst 进行sql 的优化, 也使用二进制方式存在非堆内存, 节省空间, 摆脱GC限制.  RDD 劣势就是它在jvm 中

#### 示例程序

spark.read.json("路径") 默认从file:/// 读取 , 如果spark 的conf 文件夹中有hdfs-site.xml 和core-site.xml那么会从hdfs 上面读取

```scala
val df = spark.read.json("./person.json")
val dff = spark.read.json("file:///root/person.json)
df.show
df.filter($"age"< 20).show
df.createOrReplaceTempView("people")
spark.sql("select * from people where age< 20")
```

```scala
object HelloWorld{
    def main(args: Array[String]){
        val spark = SparkSession.builder()
        .appName("sql")
        .config("","")
        .getOrCreate()
        
        val sc = spark.sparkContext
        // 添加隐式转换 可以将rdd 的操作添加到dataframe
        import spark.implicits._
        val df = spark.read.json("./person.json")
        df.show
        df.filter($"age" > 12).show
        df.createOrReplaceTempView("persons")
        spark.sql("select * from persons where age > 21").show
        spark.stop()
    }
}
```



#### spark sql 客户端

1. 可以通过spark-shell 来操作spark sql , spark 作为spark session 的变量名, sc 作为spark context 的变量名
2. 可以通过spark 提供的方法读取json 文件, 将json 文件转换成dataframe
3. 可以通过dataframe 提供的api 来操作dataframe 里面的数据
4. 可以通过dataframe 注册成为一个临时表, 来通过spark.sql 方法运行标准的sql 语句来查询

## sparksql 解析

### 创建

#### 数据源创建 

#### rdd

```scala
//rdd
val peoplerdd = sc.textFile("./person.txt")

// df --> rdd
peopledf.rdd

//dataset --> rdd
ds.rdd

```

#### data Frame

```scala
//df
val df = spark.read.json("./person.json")

// rdd ---> DataFrame
val peoplerdd = sc.textFile("./person.txt")
val peopledf = peoplerdd.map(_.split(",")).map(paras =>(paras(0), paras(1).trim().toInt)).toDF("name","age")
peopledf.show

// case class --> schema
peoplerdd.map(_.split(",")).map(paras=>Person(paras(0),paras(0).trim.toInt)).toDF  // 和rdd 转dataset 一样, toDF 不一样, 这样来获得列名字

// StructField
val schemaString = "name age"
val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true)) // 把数组每一个元素封装成StructField, // Array[Structfield]
// val fields = schemaString.split(",").map(filename => filename match{case "name" => StructField(filename, StringType, nullable = true); case "age" => StructField(filename, IntegerType, nullable = true)})
val schema = StructType(fields) // 把Array[Structfield] 传给 StructType
val rowRDD = peoplerdd.map(_.split(",")).map(att =>Row(att(0), att(1).trim))
val peopleDF = spark.createDataFrame(rowRDD, schema)

//dataset --> dataFrame
personds.toDF
```

#### dataset

```scala
// case 方法
case class Person(name:String, age: Int)
val ds = Seq(Person("Andy", 32)).toDS()
ds.show

//rdd --> dataset
val peoplerdd = sc.textFile("./person.txt")
case class Person(name:String, age: Int)
val ds  = rdd.map(_.split(",")).map(para=> Person(para(0).trim(),para(1).trim().toInt)).toDS
ds.show

//dataFrame --> dataset
df.as[Person]
```

rdd 转dataFrame 和dataSet 的不同就是map 中dataSet 需要一个case class

可以理解: dataset 就是dataframe 字段加上了属性信息

#### row

1. dataframe = dataset[row], dataframe 里面每一行都是row对象, 
2. 如果需要访问row 对象中的每一个元素, 可以通过下标row(0), 也可以通过列名字` row.getAs[String]("name")` , getAs[String] 是因为dataframe 字段没有类型

### 常用操作

两种风格 一种是dsl 风格, 需要导入import spark.implicits._*,  另一种是sql

#### dsl

```scala
import spark.implicits._
df.printSchema()
df.select("name").show
df.select($"name", $"age"+1).show
df.filter($"age" > 21).show
df.groupBy("age").count().show
```



#### sql

```scala
df.createOrReplaceTempView("people")
val sqldf = spark.sql("select * from people")
sqldf.show()
df.createGlocalTempView("people")
spark.sql("select * from global_temp.people").show
spark.newSession().sql("select * from global_temp.people").show
```

临时表是在session 范围内的, session 退出后, 表就失效了, 如果想要应用范围内有效, 可以使用全局表, 在sql 中的访问`global_temp.people`

#### 例子

```scala
val peoplerdd = sc.textFile("./person.txt")
val peopledf = peoplerdd.map(_.split(",")).map(paras=>Person(paras(0),paras(1).trim.toInt)).toDF 
peopledf.createOrReplaceTempView("persons")
val teen = spark.sql("select name, age from persons where age between 13 and 29")
teen.map(row=> "name:"+ row(0)).show
teen.map(row=> "name:"+ row.getAs[String]("name")).show

```

#### 用户滴定仪函数

#### 用户自定义UDF函数

注册使用  `spark.udf.register("名字",() =>())`

1. 通过spark.udf.register(name, func) 来注册一个udf 函数, name 是udf 函数调用时的标识符, fun 是一个函数, 用于处理字段
2. 需要将一个df 或者ds 注册为临时表
3. 通过spark.sql 运行一个sql 语句, 在sql 语句中可以通过name(列名)来应用udf 函数

```scala
val df = spark.read.json("./person.json")
spark.udf.register("addName", (x: String) => "Name: "+x)
df.createOrReplaceTempView("people")
spark.sql("select addName(name), age from people").show()
```



#### 弱类型用户自定义聚合函数  DF

强类型的DataSet 和弱类型的dataFrame 都提供相关的聚合函数, 也可以自己定义

新建一个class, 继承userDefinedAggregateFunction

```scala
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
object MyAverage extends UserDefinedAggregateFunction {
    // 聚合函数输入参数的数据类型
    def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
    // 聚合缓冲区中值得数据类型
    def bufferSchema: StructType = {
        StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
    }
    // 返回值的数据类型
    def dataType: DataType = DoubleType
    // 对于相同的输入是否一直返回相同的输出。
    def deterministic: Boolean = true
    // 初始化
    def initialize(buffer: MutableAggregationBuffer): Unit = {
        // 存工资的总额
        buffer(0) = 0L
        // 存工资的个数
        buffer(1) = 0L
    }
    // 相同 Execute 间的数据合并。
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if (!input.isNullAt(0)) {
            buffer(0) = buffer.getLong(0) + input.getLong(0)
            buffer(1) = buffer.getLong(1) + 1
        }
    }
    // 不同 Execute 间的数据合并
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
    // 计算最终结果
    def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
}
```



```shell
// 注册函数
spark.udf.register("myAverage", MyAverage)
val df = spark.read.json("examples/src/main/resources/employees.json")
df.createOrReplaceTempView("employees")
df.show()
// +-------+------+
// |
name|salary|
// +-------+------+
// |Michael| 3000|
// |
Andy| 4500|
// | Justin| 3500|
// | Berta| 4000|
// +-------+------+
val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
result.show()
// +--------------+
// |average_salary|
// +--------------+
// |
3750.0|
// +--------------+
```



#### 强类型用户自定义聚合函数  DS

通过继承Aggregator 实现强类型自定义聚合函数

```scala
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
// 既然是强类型,可能有 case 类
case class Employee(name: String, salary: Long)
case class Average(var sum: Long, var count: Long)
object MyAverage extends Aggregator[Employee, Average, Double] {
    // 定义一个数据结构,保存工资总数和工资总个数,初始都为 0
    def zero: Average = Average(0L, 0L)
    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    def reduce(buffer: Average, employee: Employee): Average = {
        buffer.sum += employee.salary
        buffer.count += 1
        buffer
    }
    // 聚合不同 execute 的结果
    def merge(b1: Average, b2: Average): Average = {
        b1.sum += b2.sum
        b1.count += b2.count
        b1
    }
    // 计算输出
    def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
    // 设定之间值类型的编码器,要转换成 case 类
    // Encoders.product 是进行 scala 元组和 case 类转换的编码器
    def bufferEncoder: Encoder[Average] = Encoders.product
    // 设定最终输出值的编码器
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
  val sparkconf = new SparkConf().setMaster("local").setAppName("test").set("spark.port.maxRetries","1000")
  val spark = SparkSession.builder().config(sparkconf).getOrCreate()
val ds = spark.read.json("examples/src/main/resources/employees.json").as[Employee]
ds.show()
// +-------+------+
// |
name|salary|
// +-------+------+
// |Michael| 3000|
// |
Andy| 4500|
// | Justin| 3500|
// | Berta| 4000|
// +-------+------+
// Convert the function to a `TypedColumn` and give it a name
val averageSalary = MyAverage.toColumn.name("average_salary")
val result = ds.select(averageSalary)
result.show()
// +--------------+
// |average_salary|
// +--------------+
// |
3750.0|
// +--------------+
```

## 数据源

### spark hive

#### 内置hive

1. 将core-site.xml和hdfs-site.xml 拷贝到spark的conf目录下。如果Spark路径下发现metastore_db，需要删除【仅第一次启动的时候】。
2. 在你第一次启动创建metastore的时候，你需要指定spark.sql.warehouse.dir这个参数，
      比如：bin/spark-shell --conf spark.sql.warehouse.dir=hdfs://hadoop-1:9000/spark_warehouse, 然后 spark.sql("show databases").show
3. 如果你在load数据的时候，需要将数据放到HDFS上。
4. 重启spark, spark-sql



#### 外置hive(优先)

1. 需要将hive-site.xml 拷贝到spark的conf目录下。
2. 如果hive的metestore使用的是mysql数据库，那么需要将mysql的jdbc驱动包放到spark的jars目录下。
3. 可以通过spark-sql或者spark-shell来进行sql的查询。完成和hive的连接。

`spark.sql("show tables").show`

#### mysql jdbc

```scala
val jdbcDF = spark.read.format("jdbc").option("url",
"jdbc:mysql://master01:3306/mysql").option("dbtable", "db").option("user",
"root").option("password", "hive").load()
val connectionProperties = new Properties()
connectionProperties.put("user", "root")
connectionProperties.put("password", "hive")
val jdbcDF2 = spark.read
.jdbc("jdbc:mysql://master01:3306/mysql", "db", connectionProperties)
// Saving data to a JDBC source
jdbcDF.write
.format("jdbc")
.option("url", "jdbc:mysql://master01:3306/mysql")
.option("dbtable", "db")
.option("user", "root")
.option("password", "hive")
.save()
jdbcDF2.write
.jdbc("jdbc:mysql://master01:3306/mysql", "db", connectionProperties)
// Specifying create table column data types on write
jdbcDF.write
.option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
.jdbc("jdbc:mysql://master01:3306/mysql", "db", connectionProperties)

```



### 输入输出

#### sparkSession.read

1、通用模式   sparkSession.read.format("json").load("path")   支持类型：parquet、json、text、csv、orc、jdbc
2、专业模式   sparkSession.read.json、 csv  直接指定类型。

#### sparkSession.write

1、通用模式   dataFrame.write.format("json").save("path")  支持类型：parquet、json、text、csv、orc、  
2、专业模式   dataFrame.write.csv("path")  直接指定类型

如果需要保存成一个text文件，那么需要dataFrame里面只有**一列**。

### 实战

```
case class tbStock(ordernumber:String,locationid:String,dateid:String) extends Serializable
val tbStockRdd = spark.sparkContext.textFile("tbStock.txt")
val tbStockDS = tbStockRdd.map(_.split(",")).map(attr=>tbStock(attr(0),attr(1),attr(2))).toDS
tbStockDS.show()

case class tbStockDetail(ordernumber:String, rownum:Int, itemid:String, number:Int,price:Double, amount:Double) extends Serializable
val tbStockDetailRdd = spark.sparkContext.textFile("tbStockDetail.txt")
val tbStockDetailDS = tbStockDetailRdd.map(_.split(",")).map(attr=> tbStockDetail(attr(0),attr(1).trim().toInt,attr(2),attr(3).trim().toInt,attr(4).trim().toDouble, attr(5).trim().toDouble)).toDS
tbStockDetailDS.show()
case class tbDate(dateid:String, years:Int, theyear:Int, month:Int, day:Int,weekday:Int, week:Int, quarter:Int, period:Int, halfmonth:Int) extends Serializable
val tbDateRdd = spark.sparkContext.textFile("tbDate.txt")
val tbDateDS =  tbDateRdd.map(_.split(",")).map(attr=>tbDate(attr(0),attr(1).trim().toInt, attr(2).trim().toInt,attr(3).trim().toInt,attr(4).trim().toInt, attr(5).trim().toInt, attr(6).trim().toInt, attr(7).trim().toInt,attr(8).trim().toInt, attr(9).trim().toInt)).toDS
tbDateDS.show()

计算所有订单中每年的销售单数、销售总额
 三表以订单号和时间连接, 然后 group by 年份,  count(distinck ordernumber), sum(amount)
spark.sql("SELECT c.theyear, COUNT(DISTINCT a.ordernumber), SUM(b.amount) FROM tbStock a JOINtbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear ORDER BY c.theyear").show

计算所有订单每年最大金额订单的销售额

第一步,  每个订单一共有多少销售额
以订单号和订单时间 group by
spark.sql("SELECT a.dateid, a.ordernumber, SUM(b.amount) AS SumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber GROUP BY a.dateid, a.ordernumber").show

每年最大金额订单的销售额
然后 使用 max (SUmOfAmount), 加上group by year, 得到一年中最大销售额
spark.sql("SELECT theyear, MAX(c.SumOfAmount) AS SumOfAmount FROM (SELECT a.dateid,a.ordernumber, SUM(b.amount) AS SumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber GROUP BY a.dateid, a.ordernumber ) c JOIN tbDate d ON c.dateid =d.dateid GROUP BY theyear ORDER BY theyear DESC").show

所有订单中每年最畅销货品
1. 求出 每个货品的销售额, group by yar, itemid,  销售额: SUM(amount)
spark.sql("SELECT c.theyear, b.itemid, SUM(b.amount) AS SumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid").show

2. 每年单个货品中的最大金额
spark.sql("SELECT d.theyear, MAX(d.SumOfAmount) AS MaxOfAmount FROM (SELECT c.theyear,b.itemid, SUM(b.amount) AS SumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber =b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) d GROUP BY d.theyear").show

最大销售额和统计好的每个货品的销售额 join,以及用年 join,集合得到最畅销货品那一行信息
spark.sql("SELECT DISTINCT e.theyear, e.itemid, f.maxofamount FROM (SELECT c.theyear,
b.itemid, SUM(b.amount) AS sumofamount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber =
b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) e JOIN
(SELECT d.theyear, MAX(d.sumofamount) AS maxofamount FROM (SELECT c.theyear, b.itemid,
SUM(b.amount) AS sumofamount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber =
b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) d GROUP BY d.theyear ) f ON e.theyear = f.theyear AND e.sumofamount = f.maxofamount ORDER BY e.theyear").show
```



























