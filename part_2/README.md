# Installation
To run this project

1. Clone the repository 
2. Download the dataset
3. Create a directory called `assets/data` on the same level as `src` and place the data files in the `data` directory
4. Install sbt ( Scala Simple Build Tool ) by following this guide: https://www.scala-sbt.org/1.x/docs/Setup.html
5. Install Spark from this page: https://spark.apache.org/downloads.html
6. From the `part_2` directory (containing the build.sbt) run the commands 
    * `sbt package`
    * `spark-submit --class yelp.part2.SentimentAnalysis --master local[4] target/scala-2.11/project-1-part-2_2.11-1.0.jar `

where `spark-submit` is in the `bin`-directory of your spark installation.


### Note
* We *strongly* recommend that you reduce standard logging level from INFO to WARN. This can be done by following this answer on StackOverflow: https://stackoverflow.com/questions/27781187/how-to-stop-info-messages-displaying-on-spark-console
* We recommend using `bash` as your shell to run this. Other shells have had various issues in our experience. 

