package bio.ferlab.etl.enriched.clinical

import bio.ferlab.etl.testmodels.enriched.{ENRICHED_SPECIMEN, ENRICHED_SPECIMEN_FAMILY}
import bio.ferlab.etl.testmodels.normalized.{NORMALIZED_BIOSPECIMEN, NORMALIZED_DISEASE, NORMALIZED_FAMILY_RELATIONSHIP, NORMALIZED_GROUP, NORMALIZED_PATIENT, NORMALIZED_PROBAND, NORMALIZED_RESEARCHSTUDY}
import bio.ferlab.etl.testmodels.{normalized, _}
import bio.ferlab.etl.testutils.WithTestSimpleConfiguration
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SpecimenEnricherSpec extends AnyFlatSpec with Matchers with WithTestSimpleConfiguration {

  import spark.implicits._

  "transform" should "enrich specimen" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_patient" -> Seq(
        NORMALIZED_PATIENT(fhir_id = "P1", `participant_id` = "PT_1"),
        NORMALIZED_PATIENT(fhir_id = "P2", `participant_id` = "PT_2", `gender` = "female"),
        NORMALIZED_PATIENT(fhir_id = "P3")
      ).toDF(),
      "normalized_family_relationship" -> Seq(
        NORMALIZED_FAMILY_RELATIONSHIP(fhir_id = "O1", participant1_fhir_id = "P1", participant2_fhir_id = "P3", participant1_to_participant_2_relationship = "mother"),
        NORMALIZED_FAMILY_RELATIONSHIP(fhir_id = "O2", participant1_fhir_id = "P2", participant2_fhir_id = "P3", participant1_to_participant_2_relationship = "father")
      ).toDF(),
      "normalized_disease" -> Seq(
        NORMALIZED_DISEASE(fhir_id = "CD1", participant_fhir_id = "P1", condition_coding = Seq(normalized.CONDITION_CODING(category = "ICD", code = "Q90.9")), `affected_status` = true),
        NORMALIZED_DISEASE(fhir_id = "CD2", participant_fhir_id = "P2", condition_coding = Seq(normalized.CONDITION_CODING(category = "ICD", code = "Q90.9"))),
        NORMALIZED_DISEASE(fhir_id = "CD3", participant_fhir_id = "P1", condition_coding = Seq(normalized.CONDITION_CODING(category = "MONDO", code = "MONDO_0002028"))),
      ).toDF(),
      "normalized_group" -> Seq(
        NORMALIZED_GROUP(fhir_id = "G1", family_members = Seq(("P1", false), ("P2", false), ("P3", false)), family_members_id = Seq("P1", "P2", "P3")),
      ).toDF(),
      "normalized_proband_observation" -> Seq(
        NORMALIZED_PROBAND(participant_fhir_id = "P3", is_proband = true),
        NORMALIZED_PROBAND(participant_fhir_id = "P2")
      ).toDF(),
      "normalized_specimen" -> Seq(
        NORMALIZED_BIOSPECIMEN(fhir_id = "S1", participant_fhir_id = "P1", `sample_id` = "BS_1", consent_type = Some("c1")),
        NORMALIZED_BIOSPECIMEN(fhir_id = "S2", participant_fhir_id = "P2", `sample_id` = "BS_2"),
        NORMALIZED_BIOSPECIMEN(fhir_id = "S3", participant_fhir_id = "P3")
      ).toDF(),
      "normalized_research_study" -> Seq(
        NORMALIZED_RESEARCHSTUDY()
      ).toDF()

    )

    val output = SpecimenEnricher(defaultRuntime, List("SD_Z6MWD3H0")).transform(data)

    val resultDF = output("enriched_specimen")

    val specimensEnriched = resultDF.as[ENRICHED_SPECIMEN].collect()
    specimensEnriched.find(_.`sample_fhir_id` == "S1") shouldBe Some(ENRICHED_SPECIMEN(`participant_fhir_id` = "P1", `participant_id` = "PT_1", `sample_fhir_id` = "S1", `sample_id` = "BS_1", consent_type = Some("c1"), `affected_status` = true, `is_proband` = false, `family` = None))
    specimensEnriched.find(_.`sample_fhir_id` == "S2") shouldBe Some(ENRICHED_SPECIMEN(`participant_fhir_id` = "P2", `participant_id` = "PT_2", `sample_fhir_id` = "S2", `sample_id` = "BS_2", `is_proband` = false, `family` = None, `gender` = "female"))
    specimensEnriched.find(_.`sample_fhir_id` == "S3") shouldBe Some(
      ENRICHED_SPECIMEN(
        family = Some(ENRICHED_SPECIMEN_FAMILY(`mother_id` = "PT_1", `father_id` = "PT_2"))
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

