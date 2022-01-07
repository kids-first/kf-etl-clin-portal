import org.apache.spark.sql.SparkSession

trait WithSparkSession {


  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.enabled", value = false)
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

}
