package bio.ferlab.etl.enrich

import bio.ferlab.etl.enrich.model._
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FamilyEnricherSpec extends AnyFlatSpec with Matchers with WithSparkSession with WithTestConfig {

  import spark.implicits._

  "transform" should "enrich family with relationship roles from a proband point of view" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_patient" -> Seq(
        PATIENT(fhir_id = "px", `participant_id` = "px"),
        PATIENT(fhir_id = "py", `participant_id` = "py", `gender` = "female"),
        PATIENT(fhir_id = "pz"),
        PATIENT(fhir_id = "ps", `participant_id` = "ps")
      ).toDF(),
      "normalized_family_relationship" -> Seq(
        OBSERVATION_FAMILY_RELATIONSHIP(fhir_id = "01", participant1_fhir_id = "py", participant2_fhir_id = "px", participant1_to_participant_2_relationship = "mother"),
        OBSERVATION_FAMILY_RELATIONSHIP(fhir_id = "02", participant1_fhir_id = "pz", participant2_fhir_id = "px", participant1_to_participant_2_relationship = "father"),
        OBSERVATION_FAMILY_RELATIONSHIP(fhir_id = "03", participant1_fhir_id = "ps", participant2_fhir_id = null, participant1_to_participant_2_relationship = null)
      ).toDF(),
      "normalized_group" -> Seq(
        GROUP(fhir_id = "gyxz", family_members = Seq(("px", false), ("px", false), ("pz", false)), family_members_id = Seq("px", "py", "pz")),
        GROUP(fhir_id = "gs", family_members = Seq(("ps", false)), family_members_id = Seq("ps")),
      ).toDF(),
      "normalized_proband_observation" -> Seq(
        OBSERVATION_PROBAND(participant_fhir_id = "px", is_proband = true),
        OBSERVATION_PROBAND(participant_fhir_id = "py"),
        OBSERVATION_PROBAND(participant_fhir_id = "pz"),
        OBSERVATION_PROBAND(participant_fhir_id = "ps", is_proband = true),
      ).toDF(),
    )

    val output = new FamilyEnricher(List("SD_Z6MWD3H0"))(conf).transform(data)

    val resultDF = output("enriched_family")

    val familyEnriched = resultDF.as[FAMILY_ENRICHED].collect()
    familyEnriched
      .find(_.proband_participant_id == "px") shouldBe Some(
      FAMILY_ENRICHED(
        family_fhir_id = "gyxz",
        proband_participant_id = "px",
        relations = Seq(
          RELATION(`fhir_id` = "pz", `role` = "father"),
          RELATION(`fhir_id` = "py", `role` = "mother"),
          RELATION(`fhir_id` = "px", `role` = "proband")
        )
    ))
    familyEnriched
      .find(_.proband_participant_id == "ps") shouldBe None
  }

}

