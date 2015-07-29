import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering.KMeans
import math._

/* INPUT: single space seperated edge list */
object CCStat {
  	def main(args: Array[String]) {
		if (args.length < 1) {
			System.err.println("Usage: CCStat <file>")
			System.exit(1)
		}

		val sparkConf = new SparkConf().setAppName("CCStat")
		val sc = new SparkContext(sparkConf)
		val data = sc.textFile(args(0)).map{
			/* last elem is dropped bec we dont take weights into account */
			line => Vectors.dense(line.split(' ').dropRight(1).map(_.toDouble))
		}.cache()

		val numIterations = 20
		/* rule of thump: sqrt(n/2) (ref: Kanti Mardia et al. (1979). Multivariate Analysis. Academic Press.) */
		val numClusters = sqrt(data.collect.toList.map(_.toArray.toList).flatMap(identity).distinct.length / 2).toInt
		var clusters = KMeans.train(data, numClusters, numIterations)
		var error = Double.PositiveInfinity
		var WSSSE = clusters.computeCost(data)

		/* the optimal k is usually one where there is an “elbow” in the WSSSE graph
		while(WSSSE < error || error == Double.PositiveInfinity) {
			error = WSSSE
			numClusters += 1
			clusters = KMeans.train(data, numClusters, numIterations)
			WSSSE = clusters.computeCost(data)
		}*/

		val vertexcids = data.map{ 
			point => val prediction = clusters.predict(point)
  			(point.toString, prediction)
		}
		
		println("Within Set Sum of Squared Errors of " + WSSSE + " is obtained using " + numClusters + " clusters")
		println(vertexcids.collect().toList)
  	}
}
