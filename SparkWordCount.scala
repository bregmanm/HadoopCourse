package example

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.streaming.flume._
import java.util.Properties

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._

object SparkWordCount {
  val conf = new SparkConf().setMaster("local[4]").setAppName("PopularHashTags").set("spark.executor.memory", "1g")
  val sc = new SparkContext(conf)

  case class Purchase(name: String, price: BigDecimal, timestamp: String,
                      category: String, ip: String)
  def parsePurchase(str: String): Purchase = {
    val fields = str.split(",", -1)
    Purchase(fields(0), BigDecimal(fields(1)).setScale(2, BigDecimal.RoundingMode.HALF_UP),
     fields(2), fields(3), fields(4))
  }

  def toLong(s: String): Long = {
    try {
      s.toLong
    } catch {
      case e: Exception => 0
    }
  }

  def toByte(s: String): Byte = {
    try {
      s.toByte
    } catch {
      case e: Exception => 0
    }
  }

  case class Location(geoname_id: Long, locale_code: String, continent_code: String,
                      continent_name: String, country_iso_code: String, country_name: String)
  def parseLocation(str: String): Location = {
    val fields = str.split(",", -1)
    Location(
      toLong(fields(0)),
      fields(1),
      fields(2), fields(3), fields(4), fields(5))
  }

  case class Block(network: String, geoname_id: Long, registered_country_geoname_id: Long,
                   represented_country_geoname_id: String, is_anonymous_proxy: Byte,
                   is_satellite_provider: Byte)
  def parseBlock(str: String): Block = {
    val fields = str.split(",", -1)
    Block(fields(0).replaceAll("/\\d+", ""),
      toLong(fields(1)),
      toLong(fields(2)), fields(3), toByte(fields(4)), toByte(fields(5)))
  }

  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val ssc = new StreamingContext(sc, Seconds(5))
    val flumeStream = FlumeUtils.createStream(ssc, "localhost", 56566)
    // Print out the count of events received from this server in each batch
    flumeStream.count().map(cnt => "Received " + cnt + " flume events." ).print()
    val purchases =
      flumeStream.map(flumeEvent => new String(flumeEvent.event.getBody.array())).map(parsePurchase)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    // Check that mysql driver exists
    Class.forName("com.mysql.jdbc.Driver")
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "cloudera")
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
    val jdbc_url = s"jdbc:mysql://localhost:3306/sqoop1"

    purchases.foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          val purchasesFrames = rdd.toDF()
          purchasesFrames.registerTempTable("purchases")
          // 10 most frequently purchased categories
          val mostFreqCat = sqlContext.sql("SELECT category, COUNT(category) AS cat_occur FROM purchases GROUP BY category ORDER BY cat_occur DESC LIMIT 10")
          mostFreqCat.show()
          mostFreqCat.write.mode(SaveMode.Overwrite)
            .jdbc(jdbc_url, "SPARK_MOST_FREQ_CAT", connectionProperties)
           // 10 most frequently purchased product in each category
          val mostFreqProdEachCat = sqlContext.sql("SELECT category, name, COUNT(name) as name_occur FROM purchases GROUP BY category, name ORDER BY category, name_occur DESC")
          mostFreqProdEachCat.show()
          mostFreqProdEachCat.write.mode(SaveMode.Overwrite)
            .jdbc(jdbc_url, "SPARK_MOST_FREQ_PROD", connectionProperties)
          // Select top 10 countries with the highest money spending
          val locationsTxtRDD = sc.textFile("/user/mbregman/geolite/locations/GeoLite2-Country-Locations-en.csv")
          val locationsTxtDataRDD = locationsTxtRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
          val locationsRdd = locationsTxtDataRDD.map(parseLocation)
          locationsRdd.toDF().registerTempTable("country_locations")
          sqlContext.sql("SELECT * FROM country_locations LIMIT 10").show()

          val blocksTxtRDD = sc.textFile("/user/mbregman/geolite/blocks/GeoLite2-Country-Blocks-IPv4.csv")
          val blocksTxtDataRDD = blocksTxtRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
          val blocksRdd = blocksTxtDataRDD.map(parseBlock)
          blocksRdd.toDF().registerTempTable("country_blocks")
          sqlContext.sql("SELECT * FROM country_blocks LIMIT 10").show()

          val countriesMostPurchased = sqlContext.sql("SELECT country_locations.country_name as country_name, ROUND(SUM(purchases.price), 2) as spend " +
            "FROM purchases, country_blocks, country_locations " +
            "WHERE purchases.ip=country_blocks.network " +
            "AND country_blocks.registered_country_geoname_id=country_locations.geoname_id " +
            "GROUP BY country_locations.country_name ORDER BY spend DESC LIMIT 10")
          countriesMostPurchased.show()
          countriesMostPurchased.write.mode(SaveMode.Overwrite)
            .jdbc(jdbc_url, "SPARK_COUNTRIES_MOST_PURCHASED", connectionProperties)

        }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def main1(args: Array[String]) {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    // Check that mysql driver exists
    Class.forName("com.mysql.jdbc.Driver")
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "cloudera")

    val jdbc_url = s"jdbc:mysql://localhost:3306/sqoop1"
    val purchaseSchema = StructType(Array(
      StructField("name", StringType, true),
      StructField("price", DecimalType(10,2), true),
      StructField("timestamp", StringType, true),
      StructField("category", StringType, true),
      StructField("ip", StringType, true)))
    val ds = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .schema(purchaseSchema)
      // Purchase class is not supported by this release
      .load("/user/cloudera/events/????/??/??/*").as[(String, BigDecimal, String, String, String)]
    // 10 most frequently purchased categories
    //SELECT category, COUNT(category) AS cat_occur FROM purchases GROUP BY category ORDER BY cat_occur DESC LIMIT 10
    //ds.groupBy(_._4).count().ord
    /*
          .toDF()
          .withColumnRenamed("value", "category")
          .withColumnRenamed("count(1)", "cat_occur")
          .orderBy($"count(1)".desc).limit(10).show()
          .write.mode(SaveMode.Overwrite)
          .jdbc(jdbc_url, "SPARK_DS_MOST_FREQ_CAT", connectionProperties)
    */

    // 10 most frequently purchased product in each category
    // SELECT category, name, COUNT(name) as name_occur FROM purchases GROUP BY category, name ORDER BY category, name_occur DESC
    //ds.groupBy($"category", $"name").count().show()
    /*


        purchases.foreachRDD(rdd => {
            if (!rdd.isEmpty()) {
              val purchasesFrames = rdd.toDF()
               // 10 most frequently purchased product in each category
              val mostFreqProdEachCat = sqlContext.sql("SELECT category, name, COUNT(name) as name_occur FROM purchases GROUP BY category, name ORDER BY category, name_occur DESC")
              mostFreqProdEachCat.show()
              mostFreqProdEachCat.write.mode(SaveMode.Overwrite)
                .jdbc(jdbc_url, "SPARK_MOST_FREQ_PROD", connectionProperties)
              // Select top 10 countries with the highest money spending
              val locationsTxtRDD = sc.textFile("/user/cloudera/geolite/locations/GeoLite2-Country-Locations-en.csv")
              val locationsTxtDataRDD = locationsTxtRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
              val locationsRdd = locationsTxtDataRDD.map(parseLocation)
              locationsRdd.toDF().registerTempTable("country_locations")
              sqlContext.sql("SELECT * FROM country_locations LIMIT 10").show()

              val blocksTxtRDD = sc.textFile("/user/cloudera/geolite/blocks/GeoLite2-Country-Blocks-IPv4.csv")
              val blocksTxtDataRDD = blocksTxtRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
              val blocksRdd = blocksTxtDataRDD.map(parseBlock)
              blocksRdd.toDF().registerTempTable("country_blocks")
              sqlContext.sql("SELECT * FROM country_blocks LIMIT 10").show()

              val countriesMostPurchased = sqlContext.sql("SELECT country_locations.country_name as country_name, SUM(purchases.price) as spend " +
                "FROM purchases, country_blocks, country_locations " +
                "WHERE purchases.ip=country_blocks.network " +
                "AND country_blocks.registered_country_geoname_id=country_locations.geoname_id " +
                "GROUP BY country_locations.country_name ORDER BY spend DESC LIMIT 10")
              countriesMostPurchased.show()
              countriesMostPurchased.write.mode(SaveMode.Overwrite)
                .jdbc(jdbc_url, "SPARK_COUNTRIES_MOST_PURCHASED", connectionProperties)

            }
    */
  }
}