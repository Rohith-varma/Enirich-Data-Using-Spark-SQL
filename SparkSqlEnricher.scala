package ca.rohith.bigdata.sparksql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSqlEnricher extends App with Log {

  val spark = SparkSession.builder()
    .appName("Spark SQL Query").master("local[*]").getOrCreate()

  val tripDf: DataFrame = spark.read.option("header", "true").option("inferschema", "true")
    .csv("/user/winter2020/rohith/assignment2/trips/trips.txt")
  val calendarDateDf: DataFrame = spark.read.option("header", "true").option("inferschema", "true")
    .csv("/user/winter2020/rohith/assignment2/calendar_dates/calendar_dates.txt")
  val routeDf = spark.read.option("header", "true").option("inferschema", "true")
    .csv("/user/winter2020/rohith/assignment2/routes/routes.txt")

  tripDf.createOrReplaceTempView("trip")
  calendarDateDf.createOrReplaceTempView("calendarDate")
  routeDf.createOrReplaceTempView("route")

  val enrichedData = spark.sql(
    """SELECT t.route_id,t.trip_headsign,t.wheelchair_accessible,
      |cd.date,cd.exception_type,r.route_long_name,
      |r.route_color FROM trip t join calendarDate cd ON
      |t.service_id=cd.service_id JOIN route r ON
      |t.route_id = r.route_id
      |""".stripMargin)

  //  tripDf.join(calendarDateDf,"service_id").join(routeDf,"route_id").show()

  enrichedData.coalesce(1).write.mode(SaveMode.Overwrite)
    .csv("/user/winter2020/rohith/assignment2/sql_enriched_json")
  spark.stop()
}
