package ca.rohith.bigdata.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlFromSource extends App with Log {

  val spark = SparkSession.builder() //initiating Spark context
    .appName("Spark SQL from Source").master("local[*]") //spark configuration
    .getOrCreate()

  //creating Dataframe from the source
  val routeDf: DataFrame = spark.read.option("header", "true").option("inferschema", "true")
    .csv("/user/winter2020/rohith/assignment2/routes/routes.txt")
  routeDf.show(10)
  routeDf.printSchema()
}
