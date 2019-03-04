import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
class test {

}
object test{
  val sparkconf = new SparkConf().setMaster("local").setAppName("test").set("spark.port.maxRetries","1000")
  val spark = SparkSession.builder().config(sparkconf).getOrCreate()
  val ds = spark.read.json("examples/src/main/resources/employees.json").as[Employee]
  ds.show()
  val averageSalary = MyAverage.toColumn.name("average_salary")
  val result = ds.select(averageSalary)
  result.show()
}

import org.apache.spark.sql.expressions.Aggregator

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
