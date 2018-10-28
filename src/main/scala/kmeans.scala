import java.lang.Math.{pow, sqrt}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.util.Random

case class Point(x: Double, y: Double) {

  def distanceTo(that: Point) = sqrt(pow(this.x - that.x, 2) + pow(this.y - that.y, 2))

  def sum(that: Point) = Point(this.x + that.x, this.y + that.y)

  def divideBy(number: Int) = Point(this.x / number, this.y / number)

  override def toString = s"$x, $y"
}

class kmeans(var sc: SparkContext, var data: RDD[Point], var K: Int) {


  var Centroids: RDD[(Point, Point)] = _

  /**
    * @param size : Number of points
    * @return A RDD map from points to their cluster
    */
  def initialize(size: Int): Unit = {
    val randomIndices = collection.mutable.HashSet[Long]()
    val random = new Random()
    while (randomIndices.size < K) {
      randomIndices += random.nextInt(size)
    }
    val initialCentroid = data.
      zipWithIndex
      .filter({ case (_, index) => randomIndices.contains(index) })
      .map({ case (point, _) => point })
      .collect().toList

    Centroids = updateCentroid(initialCentroid)
  }

  def run(): List[Point] = {
    train(findCentroids(Centroids))
  }

  def findCentroids(points: RDD[(Point, Point)]): List[Point] = {
    points.
      groupBy({
        case (point, center) => center
      }).
      map({
        case (center, points) =>
          val coordinates = points.map({ case (p, c) => p })
          val (sum, count) = coordinates.foldLeft((Point(0, 0), 0))({
            case ((acc, c), p) => (acc sum (p), c + 1)
          })
          sum divideBy (count)
      }).
      collect.toList
  }


  def updateCentroid(coordinates: List[Point]): RDD[(Point, Point)] = {
    data.map({ point =>
      val distanceToCentroids = new Ordering[Point] {
        override def compare(x: Point, y: Point): Int =
          x.distanceTo(point) compareTo y.distanceTo(point)
      }
      (point, coordinates min distanceToCentroids)
    })
  }

  @tailrec
  private def train(coordinates: List[Point]): List[Point] = {
    val newCentroids = findCentroids(updateCentroid(coordinates))
    if (newCentroids != coordinates) {
      train(newCentroids)
    } else {
      coordinates
    }

  }

}
