package ca.rohith.bigdata.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlQuery extends App with Log {

  val spark = SparkSession.builder()
    .appName("Spark SQL Query").master("local[*]").getOrCreate()

  val calendarDateDf: DataFrame = spark.read.option("header", "true").option("inferschema", "true")
    .csv("/user/winter2020/rohith/assignment2/" +
      "calendar_dates/calendar_dates.txt")

  calendarDateDf.createOrReplaceTempView("calendarDate")

  val queryResult = spark.sql(
    """SELECT service_id,exception_type
      |FROM calendarDate WHERE exception_type = 2""".stripMargin)

  queryResult.show(10)
}
