

object Q1 {
  def main(args: Array[String]) {
   
    val reviewdata = sc.textFile("/yelpdatafall/review/review.csv").map(line => line.split("\\^"))

    val rate = reviewdata.map(line => (line(2), line(3).toDouble)).reduceByKey((a, b) => a + b).distinct

    val count = reviewdata.map(line => (line(2), 1)).reduceByKey((a, b) => a + b).distinct

    val sumcount = rate.join(count)

    val result = sumcount.map(a => (a._1, a._2._1 / a._2._2))

    val sortedResult = result.takeOrdered(10)(Ordering[Double].reverse.on(_._2))

    val businessdata = sc.textFile("/yelpdatafall/business/business.csv").map(line => line.split("\\^"))

    val businessmap = businessdata.map(line => (line(0), (line(1), line(2)))).distinct

    val top10RDD = sc.parallelize(sortedResult).distinct

    val top10joinresult = businessmap.join(top10RDD)

    top10joinresult.foreach(println)
  }

}

Q1.main(null)