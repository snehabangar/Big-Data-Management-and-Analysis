

object Q4 {
   def main(args: Array[String]) {
   
    val reviewdata = sc.textFile("/yelpdatafall/review/review.csv").map(line => line.split("\\^"))

    val count = reviewdata.map(line => (line(1), 1)).reduceByKey((a, b) => a + b).distinct

    val sortedResult = count.takeOrdered(10)(Ordering[Int].reverse.on(_._2))

    val userdata = sc.textFile("/yelpdatafall/user/user.csv").map(line => line.split("\\^"))

    val usermap = userdata.map(line => (line(0), line(1))).distinct

    val top10UserReview = sc.parallelize(sortedResult).distinct

    val top10User = usermap.join(top10UserReview)
    
    val top10Username = top10User.map(x=> (x._1,x._2._1))

    top10Username.foreach(println)
  }
}

Q4.main(null)