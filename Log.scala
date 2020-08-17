package ca.rohith.bigdata.sparksql

trait Log {
  import org.apache.log4j.{Level, Logger}

  val rootLogger: Logger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

}
