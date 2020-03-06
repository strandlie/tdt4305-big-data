package yelp

import org.apache.spark.sql.SparkSession

object Task6 {


    def a (spark : SparkSession) = {
        println
        println("Task 6a:")
        val tup = Task5.a(spark)

        val reviewTableDf = tup._2
        val businessTableDf = tup._3

        businessTableDf.createOrReplaceTempView("businesses")
        reviewTableDf.createOrReplaceTempView("reviews")

        spark.sql("SELECT * FROM businesses LIMIT 5").createOrReplaceTempView("5_businesses")
        val join_result = spark.sql("SELECT * FROM businesses, reviews WHERE businesses.business_id = reviews.business_id LIMIT 50")

        println("Result of join: ")
        join_result.show
        join_result
    }

    def b(spark: SparkSession) {
        val join_result = a(spark)

        println
        println("Task 6b:")

        println("Storing temp-table: ")
        join_result.createOrReplaceTempView("businesses_reviews")
        spark.sql("DESCRIBE businesses_reviews").show

    }

    def c(spark: SparkSession) {
        println
        println("Task 6c:")
        val tup = Task5.a(spark)

        val reviewTableDf = tup._2
        reviewTableDf.createOrReplaceTempView("reviews")

        spark.sql("SELECT user_id, COUNT(review_id) AS review_count " + 
                  "FROM reviews " + 
                  "GROUP BY user_id " + 
                  "ORDER BY review_count DESC " +
                  "LIMIT 20").show
        
    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder
                        .appName("Task 6")
                        .master("local")
                        .getOrCreate()


        // a() is called in b, so no need to call it explicitly here
        b(spark)
        c(spark)
        spark.stop()
    }
}