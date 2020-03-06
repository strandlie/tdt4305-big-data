package yelp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLImplicits


object Task2 {

    def setup (spark : SparkSession) = {
        val reviews_file = spark.sparkContext.textFile("../Data/yelp_top_reviewers_with_reviews.csv").cache()
        reviews_file.map(line => line.split("\t"))
    }

    def a (spark : SparkSession) {
        println
        println("Task 2a:")
        val reviews = setup(spark)
        val ids = reviews.map (review => review(1))
            .filter(id => id != """"user_id"""")
            .map (id => if (id(0).toString == """"""") id.slice(1, id.length - 1) else id)
        val num_distinct_users = ids.distinct.count
        println("Number of distinct users: " + num_distinct_users)
    }

    def b (spark : SparkSession) {
        println
        println("Task 2b:")
        val reviews = setup(spark)
        val review_texts = reviews.map (review => review(3))
            .filter(text => { 
                try {
                    new String(java.util.Base64.getDecoder.decode(text))
                    text != """"review_text"""" 
                } catch {
                    case e: IllegalArgumentException => {
                        false
                    }
                }
            })
            .map(base64_text => { new String(java.util.Base64.getDecoder.decode(base64_text))})
        
        val character_counts = review_texts.map(text => text.length)
        val sum = character_counts.reduce(_ + _)
        val average = sum / character_counts.count
        println("Average number of characters in a user review: " + average)

    }

    def c (spark : SparkSession) {
        println
        println("Task 2c:")
        val reviews = setup(spark)
        val business_ids = reviews.map (review => review(2))
            .filter(id => id != """"business_id"""")
            .map (id => if (id(0).toString == """"""") id.slice(1, id.length - 1) else id)
        val business_id_count_ind = business_ids.map (id => (id, 1))
        val business_id_count_agg = business_id_count_ind.reduceByKey((a, b) => a + b)        
        val top_10_business_ids = business_id_count_agg.sortBy(_._2, ascending=false).take(10)
        println("The business_id's of the top 10 businesses with the highest number of reviews: ")
        top_10_business_ids.foreach(tup => println(tup._1))
    }

    def d (spark : SparkSession) {
        println
        println("Task 2d:")
        val reviews = setup(spark)
        val unix_dates = reviews.map (review => review(4))
            .filter(date => date != """"review_date"""")
            .map (date_string => if (date_string(0).toString == """"""") date_string.slice(1, date_string.length - 1) else date_string)
        val dates = unix_dates.map (date => {
            val dateInt = date.toDouble.toInt * 1000L
            val date_format = new java.text.SimpleDateFormat("yyyy")
            date_format.format(dateInt).toInt
        })
        val years_count_ind = dates.map(year => (year, 1))
        val years_count_agg = years_count_ind.reduceByKey((a, b) => a + b).sortByKey(ascending=true)
        println("The number of reviews per year: ")
        years_count_agg.take(20).foreach(println)

    }

    def e(spark : SparkSession) {
        println
        println("Task 2e:")
        val reviews = setup(spark)
        val unix_dates = reviews.map (review => review(4))
            .filter(date => date != """"review_date"""")
            .map (date_string => if (date_string(0).toString == """"""") date_string.slice(1, date_string.length - 1) else date_string)
            .map(date_string => date_string.toDouble.toInt)
        val max_date = unix_dates.reduce((a, b) => Math.max(a, b))
        val min_date = unix_dates.reduce((a, b) => Math.min(a, b))

        val df = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm")
        println("First review: " + df.format(min_date * 1000L))
        println("Last review: " + df.format(max_date * 1000L))
    }

    def f(spark : SparkSession) {
        println
        println("Task 2f:")
        val reviews = setup(spark)
        val user_ids_and_review_texts = reviews.map (review => (review(1), review(3)))
            .filter(tup => { 
                try {
                    new String(java.util.Base64.getDecoder.decode(tup._2))
                    tup._2 != """"review_text"""" 
                } catch {
                    case e: IllegalArgumentException => {
                        false
                    }
                }
            })
            .map (tup => if (tup._1(0).toString == """"""") (tup._1.slice(1, tup._1.length - 1), tup._2) else tup)
            .map(tup => { (tup._1, new String(java.util.Base64.getDecoder.decode(tup._2)))})
            .groupByKey
        user_ids_and_review_texts.cache
        
        val user_ids_and_num_reviews_and_avg_characters = user_ids_and_review_texts.map(tup => {
            val num_reviews = tup._2.toList.length
            val sum_characters = tup._2.map (review => review.length).sum
            (tup._1, num_reviews, sum_characters / num_reviews)
        })

        val number_of_users = user_ids_and_review_texts.count

        // Calculate arithmetic means for collections

        val avg_num_reviews = user_ids_and_review_texts.map(tup => tup._2.toList.length).sum / number_of_users

        val avg_num_characters = user_ids_and_review_texts.map { tup => 
            tup._2.map (_.length).sum
        }.sum / number_of_users

        // Calculate the sums

        val differences = user_ids_and_num_reviews_and_avg_characters.map { tup =>
            val num_1 = tup._2 - avg_num_reviews
            val num_2 = tup._3 - avg_num_characters
            (num_1, num_2)
        }
        differences.cache

        val numerator = differences.map(tup => tup._1 * tup._2).sum
        val denominator_1 = Math.sqrt(differences.map(tup => tup._1 * tup._1).sum)
        val denominator_2 = Math.sqrt(differences.map(tup => tup._2 * tup._2).sum)

        val PCC = numerator / (denominator_1 * denominator_2)

        println("PCC: " + PCC)
    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder
                        .master("local")
                        .appName("Task 2")
                        .getOrCreate()
       
        a(spark)
        b(spark)
        c(spark)
        d(spark)
        e(spark)
        f(spark)

        spark.stop()
    }
}
