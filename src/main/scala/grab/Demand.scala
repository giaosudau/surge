package grab

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}

/**
  * Created by giaosudau on 5/28/18.
  */
object Demand {


  def main(args: Array[String]) {
    println(args)

    val clickHouseIp = args(0)

    println("clickHouseIp ", clickHouseIp)
    val spark = SparkSession
      .builder
      .appName("DemandCount")
      .master("local[4]")
      .getOrCreate()

    val bootstrapServers: String = "13.229.98.162:9092"
    var schema: StructType = null


    schema = new StructType()
      .add("timestamp", StringType)
      .add("id", LongType)
      .add("location_id", StringType)
      .add("pickup_lat", FloatType)
      .add("pickup_lng", FloatType)
      .add("dropoff_lat", FloatType)
      .add("dropoff_lng", FloatType)

    def encodeGeohash = udf((lat: Float, lng: Float) => GeoHash.encode(lat, lng, 6))

    def decodeGeoHash = udf((geohash: String) => GeoHash.decode(geohash))


    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", "supply")
      .load()

    df.selectExpr("CAST(value AS string) AS jsonData")
      .select(from_json(col("jsonData"), schema).as("supplier"))
      .select(col("supplier.*")).withColumn("geohash", encodeGeohash(col("pickup_lat"), col("pickup_lng")))
      .withWatermark("timestamp", "10 minutes")
      .groupBy(window(col("timestamp"), "10 minutes", "5 minutes"), col("geohash"))
      .count()
      .select("window.start", "geohash", "count")

    val clickhouseUrl = "jdbc:clickhouse://%s:8123".format(clickHouseIp)

    val writer = new ClickHouseJDBCSink(clickhouseUrl, "demand")
    val output = df.writeStream
      .foreach(writer)
      .trigger(Trigger.ProcessingTime("1 minutes"))
      .outputMode("append")
      .start()
    output.awaitTermination()

  }
}