package bio.ferlab.etl.prepared.clinical

import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.etl.testmodels.enriched._
import bio.ferlab.etl.testmodels.normalized._
import bio.ferlab.etl.testmodels.prepared._
import bio.ferlab.etl.testutils.WithTestSimpleConfiguration
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SimpleParticipantSpec extends AnyFlatSpec with Matchers with WithSparkSession with WithTestSimpleConfiguration {

  import spark.implicits._

  "transform" should "prepare simple_participant" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_patient" -> Seq(
        NORMALIZED_PATIENT(fhir_id = "P1"),
        NORMALIZED_PATIENT(fhir_id = "P2"),
        NORMALIZED_PATIENT(fhir_id = "P3")
      ).toDF(),
      "normalized_vital_status" -> Seq(
        NORMALIZED_VITAL_STATUS(fhir_id = "O1", participant_fhir_id = "P1"),
        NORMALIZED_VITAL_STATUS(fhir_id = "O2", participant_fhir_id = "P2")
      ).toDF(),
      "normalized_family_relationship" -> Seq(
        NORMALIZED_FAMILY_RELATIONSHIP(fhir_id = "O1", participant1_fhir_id = "P1", participant2_fhir_id = "P3"),
        NORMALIZED_FAMILY_RELATIONSHIP(fhir_id = "O2", participant1_fhir_id = "P2", participant2_fhir_id = "P3", participant1_to_participant_2_relationship = "father"),
        NORMALIZED_FAMILY_RELATIONSHIP(fhir_id = "O2", participant1_fhir_id = "P3", participant2_fhir_id = "P1", participant1_to_participant_2_relationship = "son"),
        NORMALIZED_FAMILY_RELATIONSHIP(fhir_id = "O2", participant1_fhir_id = "P3", participant2_fhir_id = "P2", participant1_to_participant_2_relationship = "son")
      ).toDF(),
      "normalized_phenotype" -> Seq(
        NORMALIZED_PHENOTYPE(fhir_id = "CP1", participant_fhir_id = "P1", condition_coding = Seq(CONDITION_CODING(category = "HPO", code = "HP_0001631")), observed = "confirmed"),
        NORMALIZED_PHENOTYPE(fhir_id = "CP2", participant_fhir_id = "P2", condition_coding = Seq(CONDITION_CODING(category = "HPO", code = "HP_0001631")), observed = "confirmed")
      ).toDF(),
      "normalized_disease" -> Seq(
        NORMALIZED_DISEASE(fhir_id = "CD1", participant_fhir_id = "P1", condition_coding = Seq(CONDITION_CODING(category = "ICD", code = "Q90.9"))),
        NORMALIZED_DISEASE(fhir_id = "CD2", participant_fhir_id = "P2", condition_coding = Seq(CONDITION_CODING(category = "ICD", code = "Q90.9"))),
        NORMALIZED_DISEASE(fhir_id = "CD3", participant_fhir_id = "P1", condition_coding = Seq(CONDITION_CODING(category = "MONDO", code = "MONDO_0002028"))),
      ).toDF(),
      "normalized_group" -> Seq(
        NORMALIZED_GROUP(fhir_id = "G1", family_id = "G1", family_members = Seq(("P1", false), ("P2", false), ("P3", false)), family_members_id = Seq("P1", "P2", "P3")),
      ).toDF(),
      "es_index_study_centric" -> Seq(PREPARED_STUDY()).toDF(),
      "hpo_terms" -> read(getClass.getResource("/hpo_terms.json").toString, "Json", Map(), None, None),
      "mondo_terms" -> read(getClass.getResource("/mondo_terms.json.gz").toString, "Json", Map(), None, None),
      "normalized_proband_observation" -> Seq(
        NORMALIZED_PROBAND(participant_fhir_id = "P1", is_proband = true),
        NORMALIZED_PROBAND(participant_fhir_id = "P2")
      ).toDF(),
      "enriched_family" -> Seq(
        ENRICHED_FAMILY(
          family_fhir_id = "G1",
          participant_fhir_id = "P1",
          relations = Seq(
            RELATION(`participant_id` = "P1", `role` = "proband"),
            RELATION(`participant_id` = "P2", `role` = "mother"),
            RELATION(`participant_id` = "P3", `role` = "father")
          )
        ),
        ENRICHED_FAMILY(
          family_fhir_id = "G1",
          participant_fhir_id = "P2",
          relations = Seq(
            RELATION(`participant_id` = "P1", `role` = "proband"),
            RELATION(`participant_id` = "P2", `role` = "mother"),
            RELATION(`participant_id` = "P3", `role` = "father")
          )
        ),
        ENRICHED_FAMILY(
          family_fhir_id = "G1",
          participant_fhir_id = "P3",
          relations = Seq(
            RELATION(`participant_id` = "P1", `role` = "proband"),
            RELATION(`participant_id` = "P2", `role` = "mother"),
            RELATION(`participant_id` = "P3", `role` = "father")
          )
        )
      ).toDF(),
    )
    val expectedMondoTree = List(
      PREPARED_ONTOLOGY_TERM("psychiatric disorder (MONDO:0002025)", List("disease or disorder (MONDO:0000001)"), `is_tagged` = false, `is_leaf` = false, List(0)),
      PREPARED_ONTOLOGY_TERM("personality disorder (MONDO:0002028)", List("psychiatric disorder (MONDO:0002025)"), `is_tagged` = true, `is_leaf` = false, List(0)),
      PREPARED_ONTOLOGY_TERM("disease or disorder (MONDO:0000001)", List(), `is_tagged` = false, `is_leaf` = false, List(0)),
    )

    val expectedHPOTree = Seq(
      PREPARED_ONTOLOGY_TERM(name = "Abnormality of the cardiovascular system (HP:0001626)", parents = List("Phenotypic abnormality (HP:0000118)"), age_at_event_days = List(0)),
      PREPARED_ONTOLOGY_TERM(name = "All (HP:0000001)", parents = List(), age_at_event_days = List(0)),
      PREPARED_ONTOLOGY_TERM(name = "Abnormal cardiac atrium morphology (HP:0005120)", parents = List("Abnormal heart morphology (HP:0001627)"), age_at_event_days = List(0)),
      PREPARED_ONTOLOGY_TERM(name = "Abnormal cardiac septum morphology (HP:0001671)", parents = List("Abnormal heart morphology (HP:0001627)"), age_at_event_days = List(0)),
      PREPARED_ONTOLOGY_TERM(name = "Phenotypic abnormality (HP:0000118)", parents = List("All (HP:0000001)"), age_at_event_days = List(0)),
      PREPARED_ONTOLOGY_TERM(name = "Abnormal heart morphology (HP:0001627)", parents = List("Abnormality of cardiovascular system morphology (HP:0030680)"), age_at_event_days = List(0)),
      PREPARED_ONTOLOGY_TERM(name = "Abnormal atrial septum morphology (HP:0011994)", parents = List("Abnormal cardiac septum morphology (HP:0001671)"), age_at_event_days = List(0)),
      PREPARED_ONTOLOGY_TERM(name = "Abnormality of cardiovascular system morphology (HP:0030680)", parents = List("Abnormality of the cardiovascular system (HP:0001626)"), age_at_event_days = List(0)),
      PREPARED_ONTOLOGY_TERM(name = "Atrial septal defect (HP:0001631)", parents = List("Abnormal cardiac atrium morphology (HP:0005120)", "Abnormal atrial septum morphology (HP:0011994)"), is_tagged = true, age_at_event_days = List(0)))

    val output = SimpleParticipant(defaultRuntime, List("SD_Z6MWD3H0")).transform(data)
    output.keys should contain("simple_participant")

    val simple_participant = output("simple_participant").as[PREPARED_SIMPLE_PARTICIPANT].collect()

    simple_participant.length shouldBe 3

    simple_participant.find(_.fhir_id === "P1") shouldBe Some(PREPARED_SIMPLE_PARTICIPANT(
      fhir_id = "P1",
      participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "P1", participant_fhir_id_2 = "P1"),
      phenotype = Seq(PREPARED_PHENOTYPE(fhir_id = "CP1", is_observed = true)),
      observed_phenotype = expectedHPOTree,
      non_observed_phenotype = null,
      mondo = expectedMondoTree,
      diagnosis = Set(PREPARED_DIAGNOSIS(fhir_id = "CD1", icd_id_diagnosis = "Q90.9"), PREPARED_DIAGNOSIS(fhir_id = "CD3", mondo_id_diagnosis = "personality disorder (MONDO:0002028)")),
      outcomes = Seq(PREPARED_OUTCOME(fhir_id = "O1", participant_fhir_id = "P1")),
      family = PREPARED_FAMILY(
        family_id = "G1",
        relations_to_proband = Seq(
          RELATION(`participant_id` = "P1", `role` = "proband"),
          RELATION(`participant_id` = "P2", `role` = "mother"),
          RELATION(`participant_id` = "P3", `role` = "father")
        )
      ),
      family_type = "trio",
      down_syndrome_status = "D21",
      is_proband = true
    ))

    simple_participant.find(_.fhir_id === "P2") shouldBe Some(
      PREPARED_SIMPLE_PARTICIPANT(
        fhir_id = "P2",
        participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "P2", participant_fhir_id_2 = "P2"),
        phenotype = Seq(PREPARED_PHENOTYPE(fhir_id = "CP2", is_observed = true)),
        observed_phenotype = expectedHPOTree,
        non_observed_phenotype = null,
        mondo = null,
        diagnosis = Set(PREPARED_DIAGNOSIS(fhir_id = "CD2", icd_id_diagnosis = "Q90.9")),
        outcomes = Seq(PREPARED_OUTCOME(fhir_id = "O2", participant_fhir_id = "P2")),
        family = PREPARED_FAMILY(
          family_id = "G1",
          relations_to_proband = Seq(
            RELATION(`participant_id` = "P1", `role` = "proband"),
            RELATION(`participant_id` = "P2", `role` = "mother"),
            RELATION(`participant_id` = "P3", `role` = "father")
          )
        ),
        family_type = "trio"
      )
    )

    simple_participant.find(_.fhir_id === "P3") shouldBe Some(
      PREPARED_SIMPLE_PARTICIPANT(
        fhir_id = "P3",
        participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "P3", participant_fhir_id_2 = "P3"),
        phenotype = null,
        observed_phenotype = null,
        non_observed_phenotype = null,
        mondo = null,
        diagnosis = null,
        outcomes = Nil,
        family = PREPARED_FAMILY(
          family_id = "G1",
          relations_to_proband = Seq(
            RELATION(`participant_id` = "P1", `role` = "proband"),
            RELATION(`participant_id` = "P2", `role` = "mother"),
            RELATION(`participant_id` = "P3", `role` = "father")
          )
        ),
        family_type = "trio"
      )

    )
  }

}

