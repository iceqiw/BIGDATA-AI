package org.qiwei.tc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MobileRecomendDemo1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("MobileRecomendDemo1").master("local[4]")
      .getOrCreate()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val user_itemDF = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("data/fresh_comp_offline/tianchi_fresh_comp_train_user.csv")


    val extendDF = user_itemDF
      .withColumn("is_buy", when($"behavior_type" === "4", 1).otherwise(0))
      .withColumn("is_car", when($"behavior_type" === "3", 1).otherwise(0))
      .withColumn("is_favor", when($"behavior_type" === "2", 1).otherwise(0))
      .withColumn("is_browse", when($"behavior_type" === "1", 1).otherwise(0))

    extendDF.show()

    extendDF.groupBy($"user_id")
      .agg(sum("is_buy").as("is_buy"), sum("is_car").as("is_car"), sum("is_favor").as("is_favor"), sum("is_browse").as("is_browse"))
      .write.parquet("data/base/userDF")

    extendDF.groupBy($"item_id")
      .agg(sum("is_buy").as("is_buy"), sum("is_car").as("is_car"), sum("is_favor").as("is_favor"), sum("is_browse").as("is_browse"))
      .write.parquet("data/base/itemDF")

    extendDF.groupBy($"item_category")
      .agg(sum("is_buy").as("is_buy"), sum("is_car").as("is_car"), sum("is_favor").as("is_favor"), sum("is_browse").as("is_browse"))
      .write.parquet("data/base/item_categoryDF")

    extendDF.groupBy($"item_category", $"user_id")
      .agg(sum("is_buy").as("is_buy"), sum("is_car").as("is_car"), sum("is_favor").as("is_favor"), sum("is_browse").as("is_browse"))
      .write.parquet("data/base/user_categoryDF")

    spark.stop()
  }
}
