package yelp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLImplicits


object Task {
    def perform(spark : SparkSession) = {
        val reviews_file = spark.sparkContext.textFile("./assets/data/yelp_top_reviewers_with_reviews.csv")
        val lexicon_file = spark.sparkContext.textFile("./assets/resources/valence.txt")
        val stopwords_file = spark.sparkContext.textFile("./assets/resources/stopwords.txt")

        val lexicon_source = scala.io.Source.fromFile("./assets/resources/valence.txt")
        val stopwords_source = scala.io.Source.fromFile("./assets/resources/stopwords.txt")

        //val lexicon = try lexicon_source.mkString finally lexicon_source.close()
        val stopwords = try stopwords_source.mkString finally stopwords_source.close()


        println("Stopwords contains 'in': " + stopwords.contains("in"))

        val reviews = reviews_file.map(line => line.split("\t"))

        val business_ids_and_review_texts = reviews.map(review => (review(2), review(3)))
            .filter(tup => {
                try {
                    new String(java.util.Base64.getDecoder.decode(tup._2))
                    tup._2 != """review_text"""
                } catch {
                    case e: IllegalArgumentException => {
                        false
                    }
                }
            })
            .map (tup => if (tup._1(0).toString == """"""") (tup._1.slice(1, tup._1.length - 1), tup._2) else tup)
            .map(tup => { (tup._1, new String(java.util.Base64.getDecoder.decode(tup._2)))})
            .groupByKey

        val business_ids_and_tokenized_review_texts = business_ids_and_review_texts.map(id_and_reviews => {
            id_and_reviews._2.map (review_text => {
                review_text.split(" ")
            })
        })

        /*
        val without_stopwords = business_ids_and_tokenized_review_texts.map (id_and_reviews => {
            id_and_reviews._2.map (review => {
                review.filter(word => )
            })
        })*/






    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder
                        .master("local")
                        .appName("Task 2")
                        .getOrCreate()
        
        perform(spark)
        spark.stop()
    }

}