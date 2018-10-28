import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


object test {
  def main(args: Array[String]): Unit = {
    val File = Source.fromFile("/home/xi/Downloads/k-means.dat")
    val config = File.getLines().next().split(",")
    val K = config(0).toInt
    val N = config(1).toInt
    val points = File.getLines().map(line => {
      val cordinate = line.split(",")
      Point(cordinate(0).toInt,
        cordinate(1).toInt)
    }).toList
    val conf = new SparkConf().setAppName("kmeans").setMaster("local")
    val sc = new SparkContext(conf)
    val model = new kmeans(sc, sc.parallelize(points), K)
//    println(model.findCentroids(model.initialize(N)))
    //    println((K, N))
    model.initialize(N)
    println(model.run())
  }

}
