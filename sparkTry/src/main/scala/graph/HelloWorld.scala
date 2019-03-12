package graph

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("graph")
    val sc = new SparkContext(conf)

    // 顶点的集合
    val users: RDD[(VertexId,(String,String))] =sc.parallelize( Array ((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    //边的集合
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val defaultUser = ("John Doe", "Missing")
    //创建一张图, 传入顶点和边
    val graph = Graph(users, relationships, defaultUser)

    // 过滤图上的所有顶点, 如果顶点属性的第二个值是postdoc, 那么满足条件
    val verticesCount = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    // 计算满足条件的边的个数, 条件是边的源顶点ID大于目标顶点的ID
    val edgeCount = graph.edges.filter(e => e.srcId > e.dstId).count

    println(edgeCount)
    sc.stop()
  }
}
