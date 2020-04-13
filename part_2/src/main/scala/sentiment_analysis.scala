package yelp.part2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLImplicits

object SentimentAnalysis {
    def main(args: Array[String]) {
        val spark = SparkSession.builder
                        .master("local")
                        .appName("Sentiment Analysis")
                        .getOrCreate()

        Task.perform(spark)
        spark.stop()
    }
}