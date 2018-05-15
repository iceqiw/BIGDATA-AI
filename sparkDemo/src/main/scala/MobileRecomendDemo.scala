import org.apache.spark.sql.SparkSession

object MobileRecomendDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("MobileRecomendDemo").master("local[4]")
      .getOrCreate()

    //    val itemDF = spark.read.format("csv")
    //      .option("inferSchema", "true")
    //      .option("header", "true")
    //      .load("data/fresh_comp_offline/tianchi_fresh_comp_train_item.csv")
    //
    //    itemDF.show()

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

    val userBehavior = extend_userDF.groupBy("user_id", "item_category")
      .agg(
        sum("is_buy"),
        sum("is_favor"),
        sum("is_car"),
        sum("is_browse"))

    userBehavior.filter($"sum(is_buy)" === 0).filter($"sum(is_favor)" > 0).show()

    //    val userBehaviorBuy = userBehavior.groupBy("user_id", "item_category").agg(("behavior_type", "collect_set"), ("count(item_category)", "collect_set"))
    //
    //    userBehaviorBuy.show()


    spark.stop()
  }
}
