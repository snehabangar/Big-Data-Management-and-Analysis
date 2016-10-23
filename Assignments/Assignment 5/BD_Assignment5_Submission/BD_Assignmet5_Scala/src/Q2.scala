

object Q2 {
  def main(args: Array[String]) {
    
    val reviewdata = sc.textFile("/yelpdatafall/review/review.csv").map(line => line.split("\\^"))

    val rate = reviewdata.map(line => (line(1), line(3).toDouble)).reduceByKey((a, b) => a + b).distinct

    val count = reviewdata.map(line => (line(1), 1)).reduceByKey((a, b) => a + b).distinct

    val sumcount = rate.join(count)

    val result = sumcount.map(a => (a._1, a._2._1 / a._2._2))

    val name = args(0)
    
    val userdata = sc.textFile("/yelpdatafall/user/user.csv").map(line => line.split("\\^"))

    val usermap = userdata.map(line => (line(0), line(1))).filter(x=> x._2 == name)
    

    val usrrevjoin = usermap.join(result)

    usrrevjoin.foreach(println)
  }
}

Q2.main(Array("Matt J."))