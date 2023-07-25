package bio.ferlab.etl.enriched.clinical

import bio.ferlab.etl.testmodels.enriched.ENRICHED_FAMILY
import bio.ferlab.etl.testmodels.enriched._
import bio.ferlab.etl.testmodels.normalized.{NORMALIZED_FAMILY_RELATIONSHIP, NORMALIZED_GROUP, NORMALIZED_PATIENT, NORMALIZED_PROBAND}
import bio.ferlab.etl.testutils.WithTestSimpleConfiguration
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FamilyEnricherSpec extends AnyFlatSpec with Matchers with WithTestSimpleConfiguration {

  import spark.implicits._

  "transform" should "enrich family with relationship roles from a proband point of view" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_patient" -> Seq(
        NORMALIZED_PATIENT(fhir_id = "fhir_px", `participant_id` = "pt_px"),
        NORMALIZED_PATIENT(fhir_id = "fhir_py", `participant_id` = "pt_py", `gender` = "female"),
        NORMALIZED_PATIENT(fhir_id = "fhir_pz", `participant_id` = "pt_pz"),
        NORMALIZED_PATIENT(fhir_id = "fhir_ps", `participant_id` = "pt_ps")
      ).toDF(),
      "normalized_family_relationship" -> Seq(
        NORMALIZED_FAMILY_RELATIONSHIP(fhir_id = "01", participant1_fhir_id = "fhir_py", participant2_fhir_id = "fhir_px", participant1_to_participant_2_relationship = "mother"),
        NORMALIZED_FAMILY_RELATIONSHIP(fhir_id = "02", participant1_fhir_id = "fhir_pz", participant2_fhir_id = "fhir_px", participant1_to_participant_2_relationship = "father"),
        NORMALIZED_FAMILY_RELATIONSHIP(fhir_id = "03", participant1_fhir_id = "fhir_ps", participant2_fhir_id = null, participant1_to_participant_2_relationship = null)
      ).toDF(),
      "normalized_group" -> Seq(
        NORMALIZED_GROUP(fhir_id = "gyxz", family_members = Seq(("fhir_px", false), ("fhir_py", false), ("fhir_pz", false)), family_members_id = Seq("fhir_px", "fhir_py", "fhir_pz")),
        NORMALIZED_GROUP(fhir_id = "gs", family_members = Seq(("fhir_ps", false)), family_members_id = Seq("fhir_ps")),
      ).toDF(),
      "normalized_proband_observation" -> Seq(
        NORMALIZED_PROBAND(participant_fhir_id = "fhir_px", is_proband = true),
        NORMALIZED_PROBAND(participant_fhir_id = "fhir_py"),
        NORMALIZED_PROBAND(participant_fhir_id = "fhir_pz"),
        NORMALIZED_PROBAND(participant_fhir_id = "fhir_ps", is_proband = true),
      ).toDF(),
    )

    val output = FamilyEnricher(defaultRuntime,List("SD_Z6MWD3H0")).transform(data)

    val resultDF = output("enriched_family")

    val familyEnriched = resultDF.as[ENRICHED_FAMILY].collect()
    familyEnriched
      .find(_.participant_fhir_id === "fhir_pz") shouldBe Some(
      ENRICHED_FAMILY(
        family_fhir_id = "gyxz",
        participant_fhir_id = "fhir_pz",
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

