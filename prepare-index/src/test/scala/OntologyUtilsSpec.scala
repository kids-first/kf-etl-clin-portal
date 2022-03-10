import bio.ferlab.fhir.etl.common.OntologyUtils.{addDiseases, firstCategory}
import model.AGE_AT_EVENT
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.scalatest.{FlatSpec, Matchers}

class OntologyUtilsSpec extends FlatSpec with Matchers with WithSparkSession {

  case class ConditionCoding(code: String, category: String)
  import spark.implicits._


  val SCHEMA_CONDITION_CODING = "array<struct<category:string,code:string>>"

  "addDiagnosysPhenotypes" should "add diseases to dataframe" in {

    val df = Seq(
      ("part1", "diag1", "disease", Seq(("ICD", "icd"), ("MONDO", "mondo")), AGE_AT_EVENT()),
      ("part1", "diag2", "disease", Seq(("ICD", "icd")), AGE_AT_EVENT()),
      ("part2", "diag3", "disease", Seq(("ICD", "icd")), AGE_AT_EVENT()),
    ).toDF("participant_id", "diagnosis_id", "condition_profile", "condition_coding", "age_at_event")
      .withColumn("condition_coding", col("condition_coding").cast(SCHEMA_CONDITION_CODING))

    val result = addDiseases(df)
    val resultParticipant1 = result.filter(col("diagnosis_id") === "diag1").select("icd_id_diagnosis", "mondo_id_diagnosis").collect().head

    resultParticipant1 shouldBe Row("icd", "mondo")
  }

  "firstCategory" should "return the first found category" in {
    val df = Seq(Seq(("ICD", "icd"))).toDF("condition_coding").withColumn("condition_coding", col("condition_coding").cast(SCHEMA_CONDITION_CODING))
    df.withColumn("cat_icd", firstCategory("ICD", col("condition_coding"))).select("cat_icd").collect().head shouldBe Row("icd")
    df.withColumn("cat_icd", firstCategory("NotFound", col("condition_coding"))).select("cat_icd").collect().head shouldBe Row(null)
  }
}
