package ca.rohith.bigdata.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSql extends App with Log {

  val spark = SparkSession.builder() //initiating Spark context
    .appName("Spark SQL").master("local[*]") //spark configuration
    .getOrCreate()

  import spark.implicits._

  //below 3 dataframes are inferred using reflection method
  val trip: RDD[String] = spark.sparkContext.textFile("/user/winter2020/rohith/assignment2/trips/trips.txt")
  val tripRdd: RDD[Trip] = trip.filter(!_.contains("route_id")).map(Trip.apply)
  val tripDf = tripRdd.toDF

  //reading file from HDFS using spark context in spark session builder
  val calendarDate = spark.sparkContext.textFile("/user/winter2020/rohith/assignment2/calendar_dates/" +
    "calendar_dates.txt")
  val calendarDateRdd: RDD[CalendarDate] = calendarDate.filter(!_.contains("service_id"))
    .map(CalendarDate.apply)
  val calendarDateDf: DataFrame = calendarDateRdd.toDF

  val route = spark.sparkContext.textFile("/user/winter2020/rohith/assignment2/routes/routes.txt")
  val routeRdd : RDD[Route] = route.filter(!_.contains("route_id")).map(Route.apply)
  val routeDf : DataFrame = routeRdd.toDF

  routeDf.show() //by default prints 20 rows (just like println())
  routeDf.printSchema() //prints schema

  spark.stop()
}
