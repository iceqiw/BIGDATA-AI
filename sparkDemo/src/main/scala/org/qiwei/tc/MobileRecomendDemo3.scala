package org.qiwei.tc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
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
//    trainDF.filter($"label"===1).show()
    val Array(trainingDF, testDF) = trainDF.randomSplit(Array(0.9, 0.1), seed = 12345)
    val output_features= new VectorAssembler()
      .setInputCols(Array("is_buy", "is_car", "is_favor", "is0_browse", "label"))
      .setOutputCol("features")
//    trainingDF.show()
//    testDF.show()

    val lrModel = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    val pipeline= new Pipeline().setStages(Array(output_features,lrModel))

   val test1= pipeline.fit(trainingDF).transform(testDF)

    test1.filter($"label"=!=$"prediction").show
    spark.stop()
  }
}
