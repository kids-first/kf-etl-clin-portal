package bio.ferlab.etl.prepared.clinical

import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.etl.testmodels.normalized.AGE_AT_EVENT
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OntologyUtilsSpec extends AnyFlatSpec with Matchers with WithSparkSession {

  case class ConditionCoding(code: String, category: String)
  import spark.implicits._


  val SCHEMA_CONDITION_CODING = "array<struct<category:string,code:string>>"

  "addDiseases" should "add diseases to dataframe" in {

    val df = Seq(
      ("part1", "diag1", "disease", Seq(("ICD", "icd")), Some("mondo"), AGE_AT_EVENT()),
      ("part1", "diag2", "disease", Seq(("ICD", "icd")), None, AGE_AT_EVENT()),
      ("part2", "diag3", "disease", Seq(("ICD", "icd")), None, AGE_AT_EVENT()),
    ).toDF("participant_id", "diagnosis_id", "condition_profile", "condition_coding", "mondo_code", "age_at_event")
      .withColumn("condition_coding", col("condition_coding").cast(SCHEMA_CONDITION_CODING))

    val mondoTerms = Seq(("mondo", "Mondo")).toDF("id", "name")
    val result = OntologyUtils.addDiseases(df, mondoTerms)
    val resultParticipant1 = result.filter(col("diagnosis_id") === "diag1").select("icd_id_diagnosis", "mondo_id_diagnosis").collect().head

    resultParticipant1 shouldBe Row("icd", "Mondo (mondo)")
  }
}
