package grab

import java.sql.Timestamp
import java.text.SimpleDateFormat

import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties

/**
  * Created by giaosudau on 5/29/18.
  */
class ClickHouseJDBCSink(url: String, streamName: String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
  val driver = "ru.yandex.clickhouse.ClickHouseDriver"
  var connection: java.sql.Connection = _
  var statement: java.sql.Statement = _


  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    val properties = new ClickHouseProperties()

    val dataSource = new ClickHouseDataSource(url, properties)

    connection = dataSource.getConnection()
    true
  }

  def process(value: org.apache.spark.sql.Row): Unit = {
    println("\nbegin executeUpdate\n")

    var sql: String = ""
    if ("supply".equals(streamName)) {
      sql = "INSERT INTO grab.summary_test(datetime, geohash, supply_count) VALUES (?, ?, ?)"
    }
    else {
      println("customer")
      sql = "INSERT INTO grab.summary_test(datetime, geohash, customer_count) VALUES (?, ?, ?)"
    }
    val statement = connection.prepareStatement(
      sql
    )

    val timestamp: Timestamp = new Timestamp(value(0).toString.toInt)
    statement.setTimestamp(1, timestamp)
    statement.setString(2, value(1).toString)
    statement.setInt(3, value(2).toString.toInt)
    statement.executeUpdate()
    println("\ndone executeUpdate\n")

  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}

