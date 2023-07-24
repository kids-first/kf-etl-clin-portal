package bio.ferlab.etl.enrich

import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.etl.enrich.model._
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SpecimenEnricherSpec extends AnyFlatSpec with Matchers with WithSparkSession with WithTestConfig {

  import spark.implicits._

  "transform" should "enrich specimen" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_patient" -> Seq(
        PATIENT(fhir_id = "P1", `participant_id` = "PT_1"),
        PATIENT(fhir_id = "P2", `participant_id` = "PT_2", `gender` = "female"),
        PATIENT(fhir_id = "P3")
      ).toDF(),
      "normalized_family_relationship" -> Seq(
        OBSERVATION_FAMILY_RELATIONSHIP(fhir_id = "O1", participant1_fhir_id = "P1", participant2_fhir_id = "P3", participant1_to_participant_2_relationship = "mother"),
        OBSERVATION_FAMILY_RELATIONSHIP(fhir_id = "O2", participant1_fhir_id = "P2", participant2_fhir_id = "P3", participant1_to_participant_2_relationship = "father")
      ).toDF(),
      "normalized_disease" -> Seq(
        CONDITION_DISEASE(fhir_id = "CD1", participant_fhir_id = "P1", condition_coding = Seq(CONDITION_CODING(category = "ICD", code = "Q90.9")), `affected_status` = true),
        CONDITION_DISEASE(fhir_id = "CD2", participant_fhir_id = "P2", condition_coding = Seq(CONDITION_CODING(category = "ICD", code = "Q90.9"))),
        CONDITION_DISEASE(fhir_id = "CD3", participant_fhir_id = "P1", condition_coding = Seq(CONDITION_CODING(category = "MONDO", code = "MONDO_0002028"))),
      ).toDF(),
      "normalized_group" -> Seq(
        GROUP(fhir_id = "G1", family_members = Seq(("P1", false), ("P2", false), ("P3", false)), family_members_id = Seq("P1", "P2", "P3")),
      ).toDF(),
      "normalized_proband_observation" -> Seq(
        OBSERVATION_PROBAND(participant_fhir_id = "P3", is_proband = true),
        OBSERVATION_PROBAND(participant_fhir_id = "P2")
      ).toDF(),
      "normalized_specimen" -> Seq(
        BIOSPECIMEN_INPUT(fhir_id = "S1", participant_fhir_id = "P1", `sample_id` = "BS_1", consent_type = Some("c1")),
        BIOSPECIMEN_INPUT(fhir_id = "S2", participant_fhir_id = "P2", `sample_id` = "BS_2"),
        BIOSPECIMEN_INPUT(fhir_id = "S3", participant_fhir_id = "P3")
      ).toDF(),
      "normalized_research_study" -> Seq(
        RESEARCH_STUDY()
      ).toDF()

    )

    val output = new SpecimenEnricher(List("SD_Z6MWD3H0"))(conf).transform(data)

    val resultDF = output("enriched_specimen")

    val specimensEnriched = resultDF.as[SPECIMEN_ENRICHED].collect()
    specimensEnriched.find(_.`sample_fhir_id` == "S1") shouldBe Some(SPECIMEN_ENRICHED(`participant_fhir_id` = "P1", `participant_id` = "PT_1", `sample_fhir_id` = "S1", `sample_id` = "BS_1", consent_type = Some("c1"), `affected_status` = true, `is_proband` = false, `family` = None))
    specimensEnriched.find(_.`sample_fhir_id` == "S2") shouldBe Some(SPECIMEN_ENRICHED(`participant_fhir_id` = "P2", `participant_id` = "PT_2", `sample_fhir_id` = "S2", `sample_id` = "BS_2", `is_proband` = false, `family` = None, `gender` = "female"))
    specimensEnriched.find(_.`sample_fhir_id` == "S3") shouldBe Some(
      SPECIMEN_ENRICHED(
        family = Some(SPECIMEN_FAMILY_ENRICHED(`mother_id` = "PT_1", `father_id` = "PT_2"))
      )
    )

    //    ClassGenerator
    //      .writeCLassFile(
    //        "bio.ferlab.etl.enrich.model",
    //        "SPECIMEN_ENRICHED",
    //        resultDF,
    //        "enrich/src/test/scala/")
  }

}

