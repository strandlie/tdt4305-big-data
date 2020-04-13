package yelp.part2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLImplicits


object Task {
    def perform(spark : SparkSession) = {

        val reviews_file = spark.sparkContext.textFile("./assets/data/yelp_top_reviewers_with_reviews.csv")
        val lexicon_file = spark.sparkContext.textFile("./assets/resources/valence.txt")
        val stopwords_file = spark.sparkContext.textFile("./assets/resources/stopwords.txt")

        val stopwords = stopwords_file.collect.toList
        val lexicon = lexicon_file.map(line => line.split("\t"))
            .map(line => (line(0), line(1)))
            .collect
            .toMap

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
            (id_and_reviews._1, id_and_reviews._2.map(review_text => {
                val tokenized = review_text.replaceAll("[^a-zA-Z]","")
                val splitText = review_text.split(" ")
                splitText.map(word => {
                    val stripped = word.replaceAll("[^a-zA-Z]","")
                    stripped.toLowerCase
                })
                .filter(word => { word.length > 1 })
            }))
        })

        val business_ids_and_flatmapped_reviews = business_ids_and_tokenized_review_texts.map(id_and_reviews => {
            (id_and_reviews._1, id_and_reviews._2.flatMap(reviews => reviews))
        })

        val without_stopwords = business_ids_and_flatmapped_reviews.map (id_and_words => {
            (id_and_words._1, id_and_words._2.filter (word => { !stopwords.contains(word) }))
        })

        val business_ids_and_polarities = without_stopwords.map(id_and_words => {
            val business_id = id_and_words._1
            val words = id_and_words._2

            val total_polarity = words.map (word => {
                try {
                    lexicon(word).toInt
                }
                catch {
                    case e: NoSuchElementException => {
                        0
                    }
                }
            }).fold(0)(_ + _)
            
            (business_id, total_polarity)
        })

        business_ids_and_polarities.sortBy(_._2, ascending=false).take(10).foreach(println)
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