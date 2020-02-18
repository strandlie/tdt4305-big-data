import org.apache.spark

object Task2 {

    def main () {
        val reviews_file = sc.textFile("../Data/yelp_top_reviewers_with_reviews.csv")
        val reviews = reviews_file.map(_.split("\t"))
        var mapByKey = (review: Array[String]) => {
            val id = review(1)
            val rest = review.slice(0, 1) ++ review.slice(2, -1)
            (id, rest)
        }
        val reviews_by_user_id = reviews.map(mapByKey).filter(_._1 != "user_id")
        reviews_by_user_id.take(5).foreach(println)

    }
}
