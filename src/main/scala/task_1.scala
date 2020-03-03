import org.apache.spark.sql.SparkSession

object Task1 {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Project1").getOrCreate()

    val businesses = sc.textFile("yelp_businesses.csv")
    val reviewers = sc.textFile("yelp_top_reviewers_with_reviews.csv")
    val friendships = sc.textFile("yelp_top_users_friendship_graph.csv")
    
    spark.stop()
  }
}


