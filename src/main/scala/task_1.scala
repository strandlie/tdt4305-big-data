package yelp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLImplicits


object Task1 {
    def a(spark : SparkSession) = {
        println
        println("Task 1a: ")
        val reviews_file = spark.sparkContext.textFile("../Data/yelp_top_reviewers_with_reviews.csv")
        val business_file = spark.sparkContext.textFile("../Data/yelp_businesses.csv")
        val friendship_file = spark.sparkContext.textFile("../Data/yelp_top_users_friendship_graph.csv")

        println("Review table num of rows: " + reviews_file.count)
        println("Business table num of rows: " + business_file.count)
        println("Friendship graph num of rows: " + friendship_file.count)

        reviews_file.map(line => line.split("\t")).cache()
    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder
                        .master("local")
                        .appName("Task 2")
                        .getOrCreate()
        
        a(spark)
        spark.stop()
    }

}