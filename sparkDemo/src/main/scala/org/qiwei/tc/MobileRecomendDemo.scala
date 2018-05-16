package org.qiwei.tc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MobileRecomendDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("MobileRecomendDemo").master("local[4]")
      .getOrCreate()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val itemDF = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("data/fresh_comp_offline/tianchi_fresh_comp_train_item.csv")

    val item_categoryDF=itemDF.groupBy("item_category").count()
//    println(item_categoryDF.count())  //1054
//    item_categoryDF.show(3)
    /**
      * +-------------+-----+
      * |item_category|count|
      * +-------------+-----+
      * |         6658|  154|
      * |         2122|  104|
      * |         5518|    3|
      * +-------------+-----+
      */



    val userDF = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("data/fresh_comp_offline/tianchi_fresh_comp_train_user.csv")
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val start = "20141118"
    val end = "20141128"

    val extend_userDF = userDF
      .filter(date_format($"time", "yyyyMMdd") > start)
      .filter(date_format($"time", "yyyyMMdd") <= end)
      .withColumn("is_buy", when(userDF("behavior_type") === "4", 1).otherwise(0))
      .withColumn("is_favor", when(userDF("behavior_type") === "3", 1).otherwise(0))
      .withColumn("is_car", when(userDF("behavior_type") === "2", 1).otherwise(0))
      .withColumn("is_browse", when(userDF("behavior_type") === "1", 1).otherwise(0))
    //    userDF.show(1)


    //    val userCategory=userDF.groupBy("user_id","item_category","behavior_type","time").agg(("behavior_type","count"))
    //
    //    userCategory.show()
//    val k=extend_userDF.groupBy("item_category").count()
//    println(k.count()) // 8347
    val userBehavior = extend_userDF.groupBy("user_id", "item_category")
      .agg(
        sum("is_buy"),
        sum("is_favor"),
        sum("is_car"),
        sum("is_browse"))
//    println(userBehavior.count())  // 703337
    val features=item_categoryDF.join(userBehavior,item_categoryDF("item_category")===userBehavior("item_category"),"left")

    features.show()
//    userBehavior.filter($"sum(is_buy)" === 0).filter($"sum(is_favor)" > 0).show()

    //    val userBehaviorBuy = userBehavior.groupBy("user_id", "item_category").agg(("behavior_type", "collect_set"), ("count(item_category)", "collect_set"))
    //
    //    userBehaviorBuy.show()


    spark.stop()
  }
}
