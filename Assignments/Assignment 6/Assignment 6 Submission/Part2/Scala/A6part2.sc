object A6part2 {

  def main(args: Array[String]) {

    val book1 = sc.textFile("/user/ssb151030/Assignment6/Input/all-bible")

    val changeToLower = book1.map(_.toLowerCase)

    val changeToFlatMap = changeToLower.flatMap("[a-z]+".r findAllIn _)

    val limitedWords = changeToFlatMap.filter(word => word.length == 5)

    val book1Final = limitedWords.map(word => (word, 1)).reduceByKey(_ + _).sortByKey(true)

    val book2 = sc.textFile("/user/ssb151030/Assignment6/Input/shakespere")

    val changeToLower2 = book2.map(_.toLowerCase)

    val changeToFlatMap2 = changeToLower2.flatMap("[a-z]+".r findAllIn _)

    val limitedWords2 = changeToFlatMap2.filter(word => word.length == 5)

    val book2Final = limitedWords2.map(word => (word, 1)).reduceByKey(_ + _).sortByKey(true)

    val book_join = book1Final.join(book2Final)

    val sortedData = book_join.map(x => (x._2._1 + x._2._2, x._1)).sortByKey(false)

    val bookResult = sortedData.map(x => (x._2, x._1)).take(10)

    sc.makeRDD(bookResult).saveAsTextFile("Q6_Part2_Scala_result3")

  }

}



A6part2.main(null)