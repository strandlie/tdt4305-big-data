package yelp

import org.apache.spark.sql.SparkSession

object Task3 {

    def setup (spark : SparkSession) = {
        val rdd = spark.sparkContext.textFile("../../Data/yelp_businesses.csv")
                    .mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter }
                    .map(_.split("\t"))
                    .cache()
        rdd
    }

    // a) What is the average rating for businesses in each city?
    def a (spark : SparkSession) {
        println
        println("Task 3a:")
        val rdd = setup(spark)
        val city_star = rdd.map(biz => (biz(3), biz(8).toInt))
        val city_count = city_star.countByKey
        val city_accScore = city_star.reduceByKey((a,b) => a+b)
        val city_avgStars = city_accScore.map{case (city, acc) => (city, acc.toDouble/city_count(city))}
        city_avgStars.take(10).foreach(println)
        val res = city_avgStars.sortByKey(true, 1)
        // res.repartition(1).saveAsTextFile("../../../target/3a-results")
    }

    // b) What are the top 10 most frequent categories in the data?
    def b (spark : SparkSession) {
        println
        println("Task 3b:")
        val rdd = setup(spark)
        val categories_flattened = rdd.flatMap( biz => biz(10).split(", ") )
        val categories_reduced_sorted = categories_flattened.map( word => (word, 1))
                                                            .reduceByKey(_+_)
                                                            .sortBy(_._2 * -1)
        categories_reduced_sorted.take(10)foreach(println)
    }
 
    /* c)
    For each postal code in the business table, calculate the
    geographical centroid of the region shown by the postal code. 
    The geographical centroid for the points (i.e., places in a 
    region that is coded by a particular postal code) is a point 
    itself and calculated by the following formula:
    GCpostalcode=(meanpostalcode(latitude),meanpostalcode(longitude))
    */
    def c (spark : SparkSession) {
        println
        println("Task 3c:")
        val rdd = setup(spark)
        val postcode_latlong = rdd.map( biz => (biz(5), (biz(6).toFloat, biz(7).toFloat)))
        val postcode_count = postcode_latlong.countByKey
        val accumulated = postcode_latlong.reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
        val averaged = accumulated.map{ case (city,acc) => (city, (acc._1/postcode_count(city), acc._2/postcode_count(city)))}
        averaged.take(10).foreach(println)
        val res = averaged.sortByKey(true, 1)
        
        // res.repartition(1).saveAsTextFile("../../../target/3c-results")
        // averaged.sortByKey().take(20).foreach(println) // USED TO CHECK VALUES
        // postcode_latlong.sortByKey().take(20).foreach(println) // USED TO CHECK VALUES
    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder
                        .master("local")
                        .appName("Task 3")
                        .getOrCreate()
       
        a(spark)
        b(spark)
        c(spark)

        spark.stop()
    }
}
