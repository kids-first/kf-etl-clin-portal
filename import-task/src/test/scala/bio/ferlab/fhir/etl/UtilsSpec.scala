package bio.ferlab.fhir.etl

import bio.ferlab.fhir.etl.Utils.retrieveRepository
import org.apache.spark.sql.functions.col
import org.scalatest.{FlatSpec, Matchers}

class UtilsSpec extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._
  "retrieveRepository" should "return dcf, gen3 or null" in {
    val df = Seq("https://data.kidsfirstdrc.org/path", "https://api.gdc.cancer.gov/path", "other", null).toDF("repository")

    df.select(retrieveRepository(col("repository"))).as[String].collect() should contain theSameElementsAs Seq("gen3", "dcf", null, null)
  }


}
