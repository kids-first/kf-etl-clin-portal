package bio.ferlab.fhir.etl

import bio.ferlab.fhir.etl.fhavro.FhavroToNormalizedMappings.generateFhirIdColumFromIdColum
import org.apache.spark.sql.functions.col
import org.scalatest.{FlatSpec, Matchers}

class FhavroToNormalizedMappingsSpec extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  "method generateFhirIdColumFromIdColum" should "extract fhir ids when specific urls" in {
    var df = Seq(
      "http://localhost:8000/Condition/1/_history",
      "https://fhir/a/2/_history",
      "https://fhir/a/b/c/2a/_history",
      "https://fhir/123/xyz"
    ).toDF("id")

    df = df.withColumn("fhir_id", generateFhirIdColumFromIdColum())
    df.select(col("fhir_id")).as[String].collect() should contain theSameElementsAs
      Seq("1", "2", "2a", "")
  }
}
