import scala.util.Random

val randomIndices = collection.mutable.HashSet[Int]()
val random = new Random()
while (randomIndices.size < 4) {
  randomIndices += random.nextInt(100)
}

randomIndices