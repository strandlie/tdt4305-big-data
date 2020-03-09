# Installation
To run this project

1. Clone the repository 
2. Download the dataset
3. Place the data-files in a directory called ´´Data´´ at the same level as the ´´src´´ directory
4. Install sbt ( Scala Simple Build Tool ) by following this guide: https://www.scala-sbt.org/1.x/docs/Setup.html
5. Install Spark from this page: https://spark.apache.org/downloads.html
6. Run the commands 
    * ´´sbt package´´
    * ´´spark-submit --class yelp.TaskX --master local[4] target/scala-2.11/project-1_2.11-1.0.jar ´´

where ´´spark-submit´´ is in the ´´bin´´-directory of your spark installation and ´´TaskX´´ is replaced by the task you wish to execute. 

