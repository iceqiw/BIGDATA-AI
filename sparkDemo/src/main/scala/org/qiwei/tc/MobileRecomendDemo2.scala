package org.qiwei.tc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MobileRecomendDemo2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("MobileRecomendDemo2").master("local[4]")
      .getOrCreate()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val userDF = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("data/fresh_comp_offline/tianchi_fresh_comp_train_user.csv")
    val start = "20141118"
    val end = "20141120"

    val extend_userDF = userDF
      .filter(date_format($"time", "yyyyMMdd") >= start)
      .filter(date_format($"time", "yyyyMMdd") < end)
      .withColumn("is_buy", when($"behavior_type" === "4", 4).otherwise(0))
      .withColumn("is_car", when($"behavior_type" === "3", 3).otherwise(0))
      .withColumn("is_favor", when($"behavior_type" === "2", 2).otherwise(0))
      .withColumn("is_browse", when($"behavior_type" === "1", 1).otherwise(0))

    //    extend_userDF.show
    val label_userDF = userDF
      .filter(date_format($"time", "yyyyMMdd") === end)
      .withColumn("label_temp", when($"behavior_type" === "4", 1).otherwise(0))
      .select("user_id", "item_id", "label_temp")
    //    label_userDF.show()

    val user_itemDF = extend_userDF
      .join(label_userDF.select($"user_id" as "uid", $"item_id" as "iid", $"label_temp"), $"user_id" === $"uid" && $"item_id" === $"iid", "left")

    val trainDF = user_itemDF.select("user_id", "item_id", "is_buy", "is_car", "is_favor", "is_browse","label_temp")
      .withColumn("label", when($"label_temp" isNull, 0).otherwise($"label_temp"))
      .distinct()

    trainDF.select("user_id", "item_id", "is_buy", "is_car", "is_favor", "is_browse","label").write.parquet("data/train/train_d_d.parquet")
    spark.stop()
  }
}
