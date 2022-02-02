import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader}
import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.fhir.etl.centricTypes.SimpleParticipant
import model._
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

class SimpleParticipantSpec extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/dev.conf")

  "transform" should "prepare simple_participant" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_patient" -> Seq(
        PATIENT(`fhir_id` = "P1"),
        PATIENT(`fhir_id` = "P2")
      ).toDF(),
      "normalized_observation_vital-status" -> Seq(
        OBSERVATION_VITAL_STATUS(`fhir_id` = "O1", `participant_fhir_id` = "P1"),
        OBSERVATION_VITAL_STATUS(`fhir_id` = "O2", `participant_fhir_id` = "P2")
      ).toDF(),
      "normalized_condition_phenotype" -> Seq(
        CONDITION_PHENOTYPE(`fhir_id` = "CP1", `participant_fhir_id` = "P1", condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0001631")), observed = "positive"),
        CONDITION_PHENOTYPE(`fhir_id` = "CP2", `participant_fhir_id` = "P2", condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0001631")), observed = "positive")
      ).toDF(),
      "normalized_condition_disease" -> Seq(
        CONDITION_DISEASE(`fhir_id` = "CD1", `participant_fhir_id` = "P1", condition_coding = Seq(CONDITION_CODING(`category` = "ICD", `code` = "Q90.9"))),
        CONDITION_DISEASE(`fhir_id` = "CD2", `participant_fhir_id` = "P2", condition_coding = Seq(CONDITION_CODING(`category` = "ICD", `code` = "Q90.9"))),
      ).toDF(),
      "normalized_group" -> Seq(
        GROUP(`fhir_id` = "G1", `family_members` = Seq(("P1", false)), `family_members_id` = Seq("P1")),
        GROUP(`fhir_id` = "G2", `family_members` = Seq(("P2", false)), `family_members_id` = Seq("P2"))
      ).toDF(),
      "hpo_terms" -> read(getClass.getResource("/hpo_terms.json").toString, "Json", Map(), None, None),
      "mondo_terms" -> read(getClass.getResource("/mondo_terms.json").toString, "Json", Map(), None, None)
    )

    val expectedMondoTree = Seq(
      PHENOTYPE_MONDO(`name` = "Abnormality of the cardiovascular system (HP:0001626)", `parents` = List("Phenotypic abnormality (HP:0000118)"), `age_at_event_days` = List(0)),
      PHENOTYPE_MONDO(`name` = "All (HP:0000001)", `parents` = List(), `age_at_event_days` = List(0)),
      PHENOTYPE_MONDO(`name` = "Abnormal cardiac atrium morphology (HP:0005120)", `parents` = List("Abnormal heart morphology (HP:0001627)"), `age_at_event_days` = List(0)),
      PHENOTYPE_MONDO(`name` = "Abnormal cardiac septum morphology (HP:0001671)", `parents` = List("Abnormal heart morphology (HP:0001627)"), `age_at_event_days` = List(0)),
      PHENOTYPE_MONDO(`name` = "Phenotypic abnormality (HP:0000118)", `parents` = List("All (HP:0000001)"), `age_at_event_days` = List(0)),
      PHENOTYPE_MONDO(`name` = "Abnormal heart morphology (HP:0001627)", `parents` = List("Abnormality of cardiovascular system morphology (HP:0030680)"), `age_at_event_days` = List(0)),
      PHENOTYPE_MONDO(`name` = "Abnormal atrial septum morphology (HP:0011994)", `parents` = List("Abnormal cardiac septum morphology (HP:0001671)"), `age_at_event_days` = List(0)),
      PHENOTYPE_MONDO(`name` = "Abnormality of cardiovascular system morphology (HP:0030680)", `parents` = List("Abnormality of the cardiovascular system (HP:0001626)"), `age_at_event_days` = List(0)),
      PHENOTYPE_MONDO(`name` = "Atrial septal defect (HP:0001631)", `parents` = List("Abnormal cardiac atrium morphology (HP:0005120)", "Abnormal atrial septum morphology (HP:0011994)"), `is_tagged` = true, `age_at_event_days` = List(0)))

    val output = new SimpleParticipant("re_000001", List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("simple_participant")

    val simple_participant = output("simple_participant")
    simple_participant.as[SIMPLE_PARTICIPANT].collect() should contain theSameElementsAs
      Seq(
        SIMPLE_PARTICIPANT(
          `fhir_id` = "P1",
          `phenotype` = Seq(PHENOTYPE(`fhir_id` = "CP1", `observed` = true)),
          `observed_phenotype` = expectedMondoTree,
          `non_observed_phenotype` = null,
          `mondo` = null,
          `diagnosis` = Seq(DIAGNOSIS(`fhir_id` = "CD1", `icd_id_diagnosis` = "Q90.9")),
          `outcomes` = Seq(OUTCOME(`fhir_id` = "O1", `participant_fhir_id` = "P1")),
          `families` = Seq(FAMILY(`fhir_id` = "G1", `family_members` = Seq(("P1", false)))),
          `families_id` = Seq(FAMILY().`family_id`)
        ),
        SIMPLE_PARTICIPANT(
          `fhir_id` = "P2",
          `phenotype` = Seq(PHENOTYPE(`fhir_id` = "CP2", `observed` = true)),
          `observed_phenotype` = expectedMondoTree,
          `non_observed_phenotype` = null,
          `mondo` = null,
          `diagnosis` = Seq(DIAGNOSIS(`fhir_id` = "CD2", `icd_id_diagnosis` = "Q90.9")),
          `outcomes` = Seq(OUTCOME(`fhir_id` = "O2", `participant_fhir_id` = "P2")),
          `families` = Seq(FAMILY(`fhir_id` = "G2", `family_members` = Seq(("P2", false)))),
          `families_id` = Seq(FAMILY().`family_id`)
        ),
      )
  }
}

