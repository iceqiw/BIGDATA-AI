
import org.apache.spark.sql.SparkSession

object HiveDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("SELECT * FROM api_log limit 1").show()

  }

}
