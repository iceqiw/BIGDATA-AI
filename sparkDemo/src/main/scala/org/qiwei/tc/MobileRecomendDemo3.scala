package org.qiwei.tc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MobileRecomendDemo3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("MobileRecomendDemo2").master("local[4]")
      .getOrCreate()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val trainDF=spark.read.parquet("data/train/train_d_d.parquet")
    trainDF.show()
    spark.stop()
  }
}
