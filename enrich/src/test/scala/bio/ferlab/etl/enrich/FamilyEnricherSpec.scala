package bio.ferlab.etl.enrich

import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.etl.enrich.model._
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FamilyEnricherSpec extends AnyFlatSpec with Matchers with WithSparkSession with WithTestConfig {

  import spark.implicits._

  "transform" should "enrich family with relationship roles from a proband point of view" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_patient" -> Seq(
        PATIENT(fhir_id = "fhir_px", `participant_id` = "pt_px"),
        PATIENT(fhir_id = "fhir_py", `participant_id` = "pt_py", `gender` = "female"),
        PATIENT(fhir_id = "fhir_pz", `participant_id` = "pt_pz"),
        PATIENT(fhir_id = "fhir_ps", `participant_id` = "pt_ps")
      ).toDF(),
      "normalized_family_relationship" -> Seq(
        OBSERVATION_FAMILY_RELATIONSHIP(fhir_id = "01", participant1_fhir_id = "fhir_py", participant2_fhir_id = "fhir_px", participant1_to_participant_2_relationship = "mother"),
        OBSERVATION_FAMILY_RELATIONSHIP(fhir_id = "02", participant1_fhir_id = "fhir_pz", participant2_fhir_id = "fhir_px", participant1_to_participant_2_relationship = "father"),
        OBSERVATION_FAMILY_RELATIONSHIP(fhir_id = "03", participant1_fhir_id = "fhir_ps", participant2_fhir_id = null, participant1_to_participant_2_relationship = null)
      ).toDF(),
      "normalized_group" -> Seq(
        GROUP(fhir_id = "gyxz", family_members = Seq(("fhir_px", false), ("fhir_py", false), ("fhir_pz", false)), family_members_id = Seq("fhir_px", "fhir_py", "fhir_pz")),
        GROUP(fhir_id = "gs", family_members = Seq(("fhir_ps", false)), family_members_id = Seq("fhir_ps")),
      ).toDF(),
      "normalized_proband_observation" -> Seq(
        OBSERVATION_PROBAND(participant_fhir_id = "fhir_px", is_proband = true),
        OBSERVATION_PROBAND(participant_fhir_id = "fhir_py"),
        OBSERVATION_PROBAND(participant_fhir_id = "fhir_pz"),
        OBSERVATION_PROBAND(participant_fhir_id = "fhir_ps", is_proband = true),
      ).toDF(),
    )

    val output = new FamilyEnricher(List("SD_Z6MWD3H0"))(conf).transform(data)

    val resultDF = output("enriched_family")

    val familyEnriched = resultDF.as[FAMILY_ENRICHED].collect()
    familyEnriched
      .find(_.family_fhir_id === "gyxz") shouldBe Some(
      FAMILY_ENRICHED(
        family_fhir_id = "gyxz",
        relations = Seq(
          RELATION(`participant_id` = "pt_pz", `role` = "father"),
          RELATION(`participant_id` = "pt_py", `role` = "mother"),
          RELATION(`participant_id` = "pt_px", `role` = "proband")
        )
    ))
    familyEnriched
      .flatMap(_.relations.map(_.`participant_id`))
      .find(_ === "pt_ps") shouldBe None
  }

}

