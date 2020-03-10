package yelp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders


case class ReviewTableSchema(review_id: String, user_id: String, business_id: String, review_text: String, review_date: String) 
case class FriendshipTableSchema(src_user_id: String, dst_user_id: String)
case class BusinessTableSchema(business_id: String, name: String, address: String, city: String, state: String, postal_code: String, latitude: Float, longitude: Float, stars: Float, review_count: Int, categories: String)

object Task5 {

    def a (spark : SparkSession) = {
        val friendshipTableSchema = Encoders.product[FriendshipTableSchema].schema
        val friendshipTableDf = spark.read.format("csv")
                    .option("header", "true")
                    .option("delimiter", ",")
                    .schema(friendshipTableSchema)
                    .load("./assets/data/yelp_top_users_friendship_graph.csv")

        val reviewTableSchema = Encoders.product[ReviewTableSchema].schema
        val reviewTableDf = spark.read.format("csv")
                    .option("header", "true")
                    .option("delimiter", "\t")
                    .schema(reviewTableSchema)
                    .load("./assets/data/yelp_top_reviewers_with_reviews.csv")

        val businessTableSchema = Encoders.product[BusinessTableSchema].schema
        val businessTableDf = spark.read.format("csv")
                    .option("header", "true")
                    .option("delimiter", "\t")
                    .schema(businessTableSchema)
                    .load("./assets/data/yelp_businesses.csv")

        (friendshipTableDf, reviewTableDf, businessTableDf)
    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder
                        .appName("Task 5)")
                        .master("local")
                        .getOrCreate()

        a(spark)
        println
        println("Task 5a:")
        println("Running this task by itself does not produce any output in console. Please run Task6 to use the results from this task. ")
        spark.stop()


    }
}