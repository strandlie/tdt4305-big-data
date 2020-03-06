import org.apache.spark

object Task3 {

    def setup () = {
        val rdd = sc.textFile("/Users/slmdl/stuff/tdt4305-big-data/yelp_businesses.csv").mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split("\t"))
    }

    // a) What is the average rating for businesses in each city?
    def a () {
        val businesses = setup() // ER EN 'Unit = ()'
        val city_star = businesses.map(biz => (biz(3), biz(8).toInt))
        val city_count = city_star.countByKey
        val city_accScore = city_star.reduceByKey((a,b) => a+b)
        val city_avgStars = city_accScore.map{case (city, acc) => (city, acc.toDouble/city_count(city))}
        city_avgStars.take(20).foreach(println)
    }

    // b) What are the top 10 most frequent categories in the data?
    def b () {
        val businesses = setup()
        val categories_flattened = businesses.flatMap( biz => biz(10).split(", ") )
        val categories_reduced_sorted = categories_flattened.map( word => (word, 1))
                                                            .reduceByKey(_+_)
                                                            .sortBy(_._2 * -1)
        categories_reduced_sorted.take(10).foreach(println)
    }
 
    /* c)
    For each postal code in the business table, calculate the
    geographical centroid of the region shown by the postal code. 
    The geographical centroid for the points (i.e., places in a 
    region that is coded by a particular postal code) is a point 
    itself and calculated by the following formula:
    GCpostalcode=(meanpostalcode(latitude),meanpostalcode(longitude))
    */
    def c () {
        val businesses = setup()
        val postcode_latlong = businesses.map( biz => (biz(5), (biz(6).toFloat, biz(7).toFloat)))
        val postcode_count = postcode_latlong.countByKey
        val accumulated = postcode_latlong.reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
        val averaged = accumulated.map{ case (city,acc) => (city, (acc._1/postcode_count(city), acc._2/postcode_count(city)))}
        averaged.take(20).foreach(println)
        // averaged.sortByKey().take(20).foreach(println)
        // postcode_latlong.sortByKey().take(20).foreach(println)
    }
}
