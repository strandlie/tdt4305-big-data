# Installation
To run this project

1. Clone the repository 
2. Download the dataset
3. Place the data-files in a directory called `Data` at the same level as the `src` directory
4. Install sbt ( Scala Simple Build Tool ) by following this guide: https://www.scala-sbt.org/1.x/docs/Setup.html
5. Install Spark from this page: https://spark.apache.org/downloads.html
6. From the root of the repository run the commands 
    * `sbt package`
    * `spark-submit --class yelp.TaskX --master local[4] target/scala-2.11/project-1_2.11-1.0.jar `

where `spark-submit` is in the `bin`-directory of your spark installation and `TaskX` is replaced by the task you wish to execute (i.e `Task2`). 


### Note
* We *strongly* recommend that you reduce standard logging level from INFO to WARN. This can be done by following this answer on StackOverflow: https://stackoverflow.com/questions/27781187/how-to-stop-info-messages-displaying-on-spark-console
* We recommend using `bash` as your shell to run this. Other shells have had various issues in our experience. 

