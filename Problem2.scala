package comp9313.proj2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import scala.collection.mutable.ListBuffer

object Problem2 {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("problem2").setMaster("local")
    val sc = new SparkContext(conf)
//    file:///home/wjq/Desktop/tiny-graph.txt
    val inputfilePATH = args(0)
    val outputfilePATH = args(1)
    
    val edges = sc.textFile(inputfilePATH)
    val edgelist = edges.map(x => x.split(" ")).map(x=> Edge(x(1).toInt, x(2).toInt, x(0).toInt))	
    val graph = Graph.fromEdges[Int, Int](edgelist, 0);
    graph.triplets.collect().foreach(println)
    graph.triplets.map(
      triplet => triplet.srcId + " connect to  " + triplet.dstId + " with length " + triplet.attr
    ).collect.foreach(println(_))
    
    var pairs = new ListBuffer[((Int, Int))]()
    for (i <- graph.vertices.collect){
      val currentId = i._1
      val initialGraph = graph.mapVertices((id, _) => if (id == currentId) 0 else 9999)
      val sssp = initialGraph.pregel(9999)(
          // Vertex Program, if id = src, accept it
          (id, dist, newDist) => if (id == i._1 && dist == 0 && newDist != 9999) newDist else math.min(dist, newDist),
        triplet => {  // Send Message
          if (triplet.srcAttr + 1 < triplet.dstAttr || (triplet.dstId == currentId && triplet.dstAttr == 0 && triplet.srcAttr != 9999)) {
            Iterator((triplet.dstId, triplet.srcAttr + 1))
          } else {
            Iterator.empty
          }
        },
        (a, b) => math.min(a, b) // Merge Message
      )
      val pair = (currentId.toInt, sssp.vertices.filter(x => x._2 != 0 && x._2 != 9999).collect().length)
      pairs += pair
      println(sssp.vertices.filter(x => x._2 != 0).collect().length)
    }
    
//    
    println(pairs.mkString("\n"))
    val pairsrdd = sc.parallelize(pairs)
    val formatOutput = pairsrdd.sortByKey(true).map{case ((x, y)) => s"""$x:$y"""}
    formatOutput.saveAsTextFile(outputfilePATH)
    sc.stop()
  }
}