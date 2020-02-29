import org.apache.spark

object Task2 {

    def setup () = {
        val reviews_file = sc.textFile("../Data/yelp_top_reviewers_with_reviews.csv")
        reviews_file.map(_.split("\t"))
    }

    def a () {
        val reviews = setup()
        val ids = reviews.map (review => review(1))
            .filter(id => id != """"user_id"""")
        val num_distinct_users = ids.distinct.count
        println(num_distinct_users)
    }

    def b () {
        val reviews = setup()
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
        val sum = character_counts.reduce((a, b) => a + b)
        val average = sum / character_counts.count
        println(average)

    }
}
