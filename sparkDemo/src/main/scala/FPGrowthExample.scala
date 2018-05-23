// $example on$
import org.apache.spark.ml.fpm.FPGrowth
// $example off$
import org.apache.spark.sql.SparkSession

/**
  * An example demonstrating FP-Growth.
  * Run with
  * {{{
  * bin/run-example ml.FPGrowthExample
  * }}}
  */
object FPGrowthExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}").master("local")
      .getOrCreate()
    import spark.implicits._

    // $example on$
    val dataset = spark.createDataset(Seq(
      "1 2 5",
      "1 2 3 5",
      "1 2")
    ).map(t => t.split(" ")).toDF("items")

    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.5).setMinConfidence(0.6)
    val model = fpgrowth.fit(dataset)

    // Display frequent itemsets.
    model.freqItemsets.show()

    // Display generated association rules.
    model.associationRules.show()

    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    model.transform(dataset).show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
