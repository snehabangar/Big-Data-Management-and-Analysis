

object Q5 {
   def main(args: Array[String]) {
    
    val name = ", TX"
    
    val reviewdata = sc.textFile("/yelpdatafall/review/review.csv").map(line => line.split("\\^"))
    
    val reviewmap = reviewdata.map(line => (line(2),1)).reduceByKey((a, b) => a + b).distinct
    val businessdata = sc.textFile("/yelpdatafall/business/business.csv").map(line => line.split("\\^"))
    
    val businessmap = businessdata.map(line => (line(0), line(1))).filter(x=> x._2.contains(name)).distinct    
    val texasCompleteReview = businessmap.join(reviewmap);
    
    val busiReviewData = texasCompleteReview.map(x=> (x._1, x._2._2)).distinct
    
    busiReviewData.saveAsTextFile("Q5_Result_12")
  }  
}

Q5.main(null)