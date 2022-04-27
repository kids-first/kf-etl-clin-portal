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
        PATIENT(fhir_id = "P1"),
        PATIENT(fhir_id = "P2"),
        PATIENT(fhir_id = "P3")
      ).toDF(),
      "normalized_vital_status" -> Seq(
        OBSERVATION_VITAL_STATUS(fhir_id = "O1", participant_fhir_id = "P1"),
        OBSERVATION_VITAL_STATUS(fhir_id = "O2", participant_fhir_id = "P2")
      ).toDF(),
      "normalized_family_relationship" -> Seq(
        OBSERVATION_FAMILY_RELATIONSHIP(fhir_id = "O1", participant1_fhir_id = "P1", participant2_fhir_id = "P3"),
        OBSERVATION_FAMILY_RELATIONSHIP(fhir_id = "O2", participant1_fhir_id = "P2", participant2_fhir_id = "P3", participant1_to_participant_2_relationship = "father"),
        OBSERVATION_FAMILY_RELATIONSHIP(fhir_id = "O2", participant1_fhir_id = "P3", participant2_fhir_id = "P1", participant1_to_participant_2_relationship = "son"),
        OBSERVATION_FAMILY_RELATIONSHIP(fhir_id = "O2", participant1_fhir_id = "P3", participant2_fhir_id = "P2", participant1_to_participant_2_relationship = "son")
      ).toDF(),
      "normalized_phenotype" -> Seq(
        CONDITION_PHENOTYPE(fhir_id = "CP1", participant_fhir_id = "P1", condition_coding = Seq(CONDITION_CODING(category = "HPO", code = "HP_0001631")), observed = "confirmed"),
        CONDITION_PHENOTYPE(fhir_id = "CP2", participant_fhir_id = "P2", condition_coding = Seq(CONDITION_CODING(category = "HPO", code = "HP_0001631")), observed = "confirmed")
      ).toDF(),
      "normalized_disease" -> Seq(
        CONDITION_DISEASE(fhir_id = "CD1", participant_fhir_id = "P1", condition_coding = Seq(CONDITION_CODING(category = "ICD", code = "Q90.9"))),
        CONDITION_DISEASE(fhir_id = "CD2", participant_fhir_id = "P2", condition_coding = Seq(CONDITION_CODING(category = "ICD", code = "Q90.9"))),
        CONDITION_DISEASE(fhir_id = "CD3", participant_fhir_id = "P1", condition_coding = Seq(CONDITION_CODING(category = "MONDO", code = "MONDO_0008608"))),
      ).toDF(),
      "normalized_group" -> Seq(
        GROUP(fhir_id = "G1", family_members = Seq(("P1", false), ("P2", false), ("P3", false)), family_members_id = Seq("P1", "P2", "P3")),
      ).toDF(),
      "es_index_study_centric" -> Seq(STUDY_CENTRIC()).toDF(),
      "hpo_terms" -> read(getClass.getResource("/hpo_terms.json").toString, "Json", Map(), None, None),
      "mondo_terms" -> read(getClass.getResource("/mondo_terms.json").toString, "Json", Map(), None, None)
    )
    val expectedMondoTree = List(
      PHENOTYPE_ENRICHED("disorder of development or morphogenesis (MONDO:0021147)", List("disease or disorder (MONDO:0000001)"), false, false, List(0)),
      PHENOTYPE_ENRICHED("total autosomal trisomy (MONDO:0020051)", List("autosomal trisomy (MONDO:0020050)"), false, false, List(0)),
      PHENOTYPE_ENRICHED("inherited genetic disease (MONDO:0003847)", List("disease or disorder (MONDO:0000001)"), false, false, List(0)),
      PHENOTYPE_ENRICHED("chromosomal anomaly (MONDO:0019040)", List("inherited genetic disease (MONDO:0003847)", "developmental defect during embryogenesis (MONDO:0019755)"), false, false, List(0)),
      PHENOTYPE_ENRICHED("Down syndrome (MONDO:0008608)", List("total autosomal trisomy (MONDO:0020051)"), true, true, List(0)),
      PHENOTYPE_ENRICHED("developmental defect during embryogenesis (MONDO:0019755)", List("congenital abnormality (MONDO:0000839)", "disorder of development or morphogenesis (MONDO:0021147)"), false, false, List(0)),
      PHENOTYPE_ENRICHED("autosomal anomaly (MONDO:0020049)", List("chromosomal anomaly (MONDO:0019040)"), false, false, List(0)),
      PHENOTYPE_ENRICHED("disease or disorder (MONDO:0000001)", List(), false, false, List(0)),
      PHENOTYPE_ENRICHED("congenital abnormality (MONDO:0000839)", List("disease or disorder (MONDO:0000001)"), false, false, List(0)),
      PHENOTYPE_ENRICHED("autosomal trisomy (MONDO:0020050)", List("autosomal anomaly (MONDO:0020049)"), false, false, List(0)))

    val expectedHPOTree = Seq(
      PHENOTYPE_ENRICHED(name = "Abnormality of the cardiovascular system (HP:0001626)", parents = List("Phenotypic abnormality (HP:0000118)"), age_at_event_days = List(0)),
      PHENOTYPE_ENRICHED(name = "All (HP:0000001)", parents = List(), age_at_event_days = List(0)),
      PHENOTYPE_ENRICHED(name = "Abnormal cardiac atrium morphology (HP:0005120)", parents = List("Abnormal heart morphology (HP:0001627)"), age_at_event_days = List(0)),
      PHENOTYPE_ENRICHED(name = "Abnormal cardiac septum morphology (HP:0001671)", parents = List("Abnormal heart morphology (HP:0001627)"), age_at_event_days = List(0)),
      PHENOTYPE_ENRICHED(name = "Phenotypic abnormality (HP:0000118)", parents = List("All (HP:0000001)"), age_at_event_days = List(0)),
      PHENOTYPE_ENRICHED(name = "Abnormal heart morphology (HP:0001627)", parents = List("Abnormality of cardiovascular system morphology (HP:0030680)"), age_at_event_days = List(0)),
      PHENOTYPE_ENRICHED(name = "Abnormal atrial septum morphology (HP:0011994)", parents = List("Abnormal cardiac septum morphology (HP:0001671)"), age_at_event_days = List(0)),
      PHENOTYPE_ENRICHED(name = "Abnormality of cardiovascular system morphology (HP:0030680)", parents = List("Abnormality of the cardiovascular system (HP:0001626)"), age_at_event_days = List(0)),
      PHENOTYPE_ENRICHED(name = "Atrial septal defect (HP:0001631)", parents = List("Abnormal cardiac atrium morphology (HP:0005120)", "Abnormal atrial septum morphology (HP:0011994)"), is_tagged = true, age_at_event_days = List(0)))

    val output = new SimpleParticipant("re_000001", List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("simple_participant")

    val simple_participant = output("simple_participant").as[SIMPLE_PARTICIPANT].collect()

    simple_participant.length shouldBe 3

    simple_participant.find(_.fhir_id === "P1") shouldBe Some(SIMPLE_PARTICIPANT(
      fhir_id = "P1",
      participant_fhir_id = "P1",
      phenotype = Seq(PHENOTYPE(fhir_id = "CP1", is_observed = true)),
      observed_phenotype = expectedHPOTree,
      non_observed_phenotype = null,
      mondo = expectedMondoTree,
      diagnosis = Set(DIAGNOSIS(fhir_id = "CD3", mondo_id_diagnosis = "Down syndrome (MONDO:0008608)"), DIAGNOSIS(fhir_id = "CD1", icd_id_diagnosis = "Q90.9")),
      outcomes = Seq(OUTCOME(fhir_id = "O1", participant_fhir_id = "P1")),
      family = FAMILY(fhir_id = "G1", family_relations = Seq(FAMILY_RELATIONS(related_participant_fhir_id = "P3", relation = "son"))),
      family_type = "trio",
      down_syndrome_status = "T21",
      down_syndrome_diagnosis = List("Down syndrome (MONDO:0008608)")
    ))

    simple_participant.find(_.fhir_id === "P2") shouldBe Some(
      SIMPLE_PARTICIPANT(
        fhir_id = "P2",
        participant_fhir_id = "P2",
        phenotype = Seq(PHENOTYPE(fhir_id = "CP2", is_observed = true)),
        observed_phenotype = expectedHPOTree,
        non_observed_phenotype = null,
        mondo = null,
        diagnosis = Set(DIAGNOSIS(fhir_id = "CD2", icd_id_diagnosis = "Q90.9")),
        outcomes = Seq(OUTCOME(fhir_id = "O2", participant_fhir_id = "P2")),
        family = FAMILY(fhir_id = "G1", family_relations = Seq(FAMILY_RELATIONS(related_participant_fhir_id = "P3", relation = "son"))),
        family_type = "trio"
      )
    )

    simple_participant.find(_.fhir_id === "P3") shouldBe Some(
      SIMPLE_PARTICIPANT(
        fhir_id = "P3",
        participant_fhir_id = "P3",
        phenotype = null,
        observed_phenotype = null,
        non_observed_phenotype = null,
        mondo = null,
        diagnosis = null,
        outcomes = Nil,
        family = FAMILY(fhir_id = "G1", father_id = Some("PT_48DYT4PP"), mother_id = Some("PT_48DYT4PP"), family_relations = Seq(FAMILY_RELATIONS(related_participant_fhir_id = "P2", relation = "father"), FAMILY_RELATIONS(related_participant_fhir_id = "P1", relation = "mother"))),
        family_type = "trio"
      )

    )
  }

}

