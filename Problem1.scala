package comp9313.proj2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer

object Problem1 {

  def main(args: Array[String]) {
    val k = args(0)
    val stopWordPATH = args(1)
    val inputFilePATH = args(2)
    val outputFolderPATH = args(3)
    val conf = new SparkConf().setAppName("LetterCount").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFilePATH)
    val stopWordsFile = sc.textFile(stopWordPATH).collect.toList
    
    val words = textFile.map(_.split(",")).map(x => x.tail).map(x => x(0).split(" ")).collect()
    
    val finallist = words.map(x => x.filterNot(stopWordsFile.contains(_))).map(x => x.
      filter(x => x.charAt(0) <='z' && x.charAt(0) >='a'))
    
    var pairs = new ListBuffer[((String, String), Int)]()
    for (line <- finallist) {
      val i = 0
      for (i <- 0 until line.size) {
        val j = i + 1
        for (j <- i + 1 until line.size) {
          if (line(i) < line(j)) {
            val pair = ((line(i), line(j)), 1)
            pairs += pair
          }
          else{
            val pair = ((line(j), line(i)), 1)
            pairs += pair
          }
        }
      }
    }
    
    val pairsrdd = sc.parallelize(pairs)
    var output = pairsrdd.reduceByKey(_+_).map(_.swap).sortByKey(false).map(_.swap).take(k.toInt)
    val formatOutput = output.map{case ((x, y),z) => s"""$x,$y\t$z"""}
    val outrdd = sc.parallelize(formatOutput)
    outrdd.saveAsTextFile(outputFolderPATH)
//    outrdd.foreach(println)
    sc.stop()
  }
}