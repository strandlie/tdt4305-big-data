import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val conf = new SparkConf().setAppName("Project1_b").setMaster("local")
val sc = new SparkContext(conf)



