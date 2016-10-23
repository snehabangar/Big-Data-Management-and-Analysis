

object Q3 {
  def main(args: Array[String]) {
    
    val name = "Stanford"
    
    val reviewdata = sc.textFile("/yelpdatafall/review/review.csv").map(line => line.split("\\^"))
    
    val reviewmap = reviewdata.map(line => (line(2), (line(1),line(3).toDouble))).distinct

   
    val businessdata = sc.textFile("/yelpdatafall/business/business.csv").map(line => line.split("\\^"))
    
    val businessmap = businessdata.map(line => (line(0), line(1))).filter(x=> x._2.contains(name)).distinct
    
    val stanfordCompleteReview = businessmap.join(reviewmap);
    
    val userReviewData = stanfordCompleteReview.map(_._2._2)
    
    userReviewData.saveAsTextFile("Q3_Result_1")
  }  
}

Q3.main(null)