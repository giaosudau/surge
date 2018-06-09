package grab

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}

/**
  * Created by giaosudau on 5/28/18.
  */
object Supply extends Logging{


  def main(args: Array[String]) {

    println(args)

    val clickHouseIp = args(0)

    println("clickHouseIp ", clickHouseIp)
    val spark = SparkSession
      .builder
      .appName("SupplyCount")
      .master("local[4]")
      .getOrCreate()

    val bootstrapServers: String = "13.229.98.162:9092"
    var schema: StructType = null
    var stream: DataFrame = null


    schema = new StructType()
      .add("timestamp", TimestampType)
      .add("supplier_id", LongType)
      .add("location_id", StringType)
      .add("lat", FloatType)
      .add("lng", FloatType)
    //      schema = new StructType()
    //        .add("timestamp", StringType)
    //        .add("id", LongType)
    //        .add("location_id", StringType)
    //        .add("pickup_lat", FloatType)
    //        .add("dropoff_lat", FloatType)
    //        .add("dropoff_lng", FloatType)

    def encodeGeohash = udf((lat: Float, lng: Float) => GeoHash.encode(lat, lng, 6))

    def decodeGeoHash = udf((geohash: String) => GeoHash.decode(geohash))


    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", "supply")
      .load()

    val agg_df = df.selectExpr("CAST(value AS string) AS jsonData")
      .select(from_json(col("jsonData"), schema).as("supplier"))
      .select(col("supplier.*")).withColumn("geohash", encodeGeohash(col("lat"), col("lng")))
      .withWatermark("timestamp", "10 minutes")
      .groupBy(window(col("timestamp"), "10 minutes"), col("geohash"))
      .count()
      .select("window.start", "geohash", "count")


    val clickhouseUrl = "jdbc:clickhouse://%s:8123".format(clickHouseIp)
    val writer = new ClickHouseJDBCSink(clickhouseUrl, "supply")
    val output = agg_df.writeStream
      .foreach(writer)
      .trigger(Trigger.ProcessingTime("10 minutes"))
      .outputMode("append")
      .start()
    output.awaitTermination()

  }
}

object GeoHash {

  val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

  def encode(lat: Double, lng: Double): String = encode(lat, lng, 12)

  def encode(lat: Double, lng: Double, precision: Int): String = {

    var (minLat, maxLat) = (-90.0, 90.0)
    var (minLng, maxLng) = (-180.0, 180.0)
    val bits = List(16, 8, 4, 2, 1)

    (0 until precision).map { p => {
      base32 apply (0 until 5).map { i => {
        if (((5 * p) + i) % 2 == 0) {
          val mid = (minLng + maxLng) / 2.0
          if (lng > mid) {
            minLng = mid
            bits(i)
          } else {
            maxLng = mid
            0
          }
        } else {
          val mid = (minLat + maxLat) / 2.0
          if (lat > mid) {
            minLat = mid
            bits(i)
          } else {
            maxLat = mid
            0
          }
        }
      }
      }.reduceLeft((a, b) => a | b)
    }
    }.mkString("")
  }

  def decode(geohash: String): ((Double, Double), (Double, Double)) = {
    def toBitList(s: String) = s.flatMap {
      c => ("00000" + base32.indexOf(c).toBinaryString).takeRight(5).map('1' ==)
    } toList

    def split(l: List[Boolean]): (List[Boolean], List[Boolean]) = {
      l match {
        case Nil => (Nil, Nil)
        case x :: Nil => (x :: Nil, Nil)
        case x :: y :: zs => val (xs, ys) = split(zs); (x :: xs, y :: ys)
      }
    }

    def dehash(xs: List[Boolean], min: Double, max: Double): (Double, Double) = {
      ((min, max) /: xs) {
        case ((min, max), b) =>
          if (b) ((min + max) / 2, max)
          else (min, (min + max) / 2)
      }
    }

    val (xs, ys) = split(toBitList(geohash))
    (dehash(xs, -180, 180), dehash(ys, -90, 90))
  }
}