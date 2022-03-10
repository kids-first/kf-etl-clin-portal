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
        PATIENT(`fhir_id` = "P2"),
        PATIENT(`fhir_id` = "P3")
      ).toDF(),
      "normalized_vital_status" -> Seq(
        OBSERVATION_VITAL_STATUS(`fhir_id` = "O1", `participant_fhir_id` = "P1"),
        OBSERVATION_VITAL_STATUS(`fhir_id` = "O2", `participant_fhir_id` = "P2")
      ).toDF(),
      "normalized_family_relationship" -> Seq(
        OBSERVATION_FAMILY_RELATIONSHIP(`fhir_id` = "O1", `participant1_fhir_id` = "P1", `participant2_fhir_id` = "P3"),
        OBSERVATION_FAMILY_RELATIONSHIP(`fhir_id` = "O2", `participant1_fhir_id` = "P2", `participant2_fhir_id` = "P3", `participant1_to_participant_2_relationship` = "father"),
        OBSERVATION_FAMILY_RELATIONSHIP(`fhir_id` = "O2", `participant1_fhir_id` = "P3", `participant2_fhir_id` = "P1", `participant1_to_participant_2_relationship` = "son"),
        OBSERVATION_FAMILY_RELATIONSHIP(`fhir_id` = "O2", `participant1_fhir_id` = "P3", `participant2_fhir_id` = "P2", `participant1_to_participant_2_relationship` = "son")
      ).toDF(),
      "normalized_phenotype" -> Seq(
        CONDITION_PHENOTYPE(`fhir_id` = "CP1", `participant_fhir_id` = "P1", condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0001631")), observed = "confirmed"),
        CONDITION_PHENOTYPE(`fhir_id` = "CP2", `participant_fhir_id` = "P2", condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0001631")), observed = "confirmed")
      ).toDF(),
      "normalized_disease" -> Seq(
        CONDITION_DISEASE(`fhir_id` = "CD1", `participant_fhir_id` = "P1", condition_coding = Seq(CONDITION_CODING(`category` = "ICD", `code` = "Q90.9"))),
        CONDITION_DISEASE(`fhir_id` = "CD2", `participant_fhir_id` = "P2", condition_coding = Seq(CONDITION_CODING(`category` = "ICD", `code` = "Q90.9"))),
      ).toDF(),
      "normalized_group" -> Seq(
        GROUP(`fhir_id` = "G1", `family_members` = Seq(("P1", false), ("P2", false), ("P3", false)), `family_members_id` = Seq("P1", "P2", "P3")),
      ).toDF(),
      "es_index_study_centric" -> Seq(STUDY_CENTRIC()).toDF(),
      "hpo_terms" -> read(getClass.getResource("/hpo_terms.json").toString, "Json", Map(), None, None),
      "mondo_terms" -> read(getClass.getResource("/mondo_terms.json").toString, "Json", Map(), None, None)
    )

    val expectedMondoTree = Seq(
      PHENOTYPE_ENRICHED(`name` = "Abnormality of the cardiovascular system (HP:0001626)", `parents` = List("Phenotypic abnormality (HP:0000118)"), `age_at_event_days` = List(0)),
      PHENOTYPE_ENRICHED(`name` = "All (HP:0000001)", `parents` = List(), `age_at_event_days` = List(0)),
      PHENOTYPE_ENRICHED(`name` = "Abnormal cardiac atrium morphology (HP:0005120)", `parents` = List("Abnormal heart morphology (HP:0001627)"), `age_at_event_days` = List(0)),
      PHENOTYPE_ENRICHED(`name` = "Abnormal cardiac septum morphology (HP:0001671)", `parents` = List("Abnormal heart morphology (HP:0001627)"), `age_at_event_days` = List(0)),
      PHENOTYPE_ENRICHED(`name` = "Phenotypic abnormality (HP:0000118)", `parents` = List("All (HP:0000001)"), `age_at_event_days` = List(0)),
      PHENOTYPE_ENRICHED(`name` = "Abnormal heart morphology (HP:0001627)", `parents` = List("Abnormality of cardiovascular system morphology (HP:0030680)"), `age_at_event_days` = List(0)),
      PHENOTYPE_ENRICHED(`name` = "Abnormal atrial septum morphology (HP:0011994)", `parents` = List("Abnormal cardiac septum morphology (HP:0001671)"), `age_at_event_days` = List(0)),
      PHENOTYPE_ENRICHED(`name` = "Abnormality of cardiovascular system morphology (HP:0030680)", `parents` = List("Abnormality of the cardiovascular system (HP:0001626)"), `age_at_event_days` = List(0)),
      PHENOTYPE_ENRICHED(`name` = "Atrial septal defect (HP:0001631)", `parents` = List("Abnormal cardiac atrium morphology (HP:0005120)", "Abnormal atrial septum morphology (HP:0011994)"), `is_tagged` = true, `age_at_event_days` = List(0)))

    val output = new SimpleParticipant("re_000001", List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("simple_participant")

    val simple_participant = output("simple_participant").as[SIMPLE_PARTICIPANT].collect()

    simple_participant.length shouldBe 3

    simple_participant.find(_.`fhir_id` === "P1") shouldBe Some(SIMPLE_PARTICIPANT(
      `fhir_id` = "P1",
      `phenotype` = Seq(PHENOTYPE(`fhir_id` = "CP1", `is_observed` = true)),
      `observed_phenotype` = expectedMondoTree,
      `non_observed_phenotype` = null,
      `mondo` = null,
      `diagnosis` = Seq(DIAGNOSIS(`fhir_id` = "CD1", `icd_id_diagnosis` = "Q90.9")),
      `outcomes` = Seq(OUTCOME(`fhir_id` = "O1", `participant_fhir_id` = "P1")),
      `family` = FAMILY(`fhir_id` = "G1", `family_members_id` = Seq("P1", "P2", "P3"), `family_relations` = Seq(FAMILY_RELATIONS("P3", "son"))),
      `family_type` = "other"
    ))

    simple_participant.find(_.`fhir_id` === "P2") shouldBe Some(
      SIMPLE_PARTICIPANT(
        `fhir_id` = "P2",
        `phenotype` = Seq(PHENOTYPE(`fhir_id` = "CP2", `is_observed` = true)),
        `observed_phenotype` = expectedMondoTree,
        `non_observed_phenotype` = null,
        `mondo` = null,
        `diagnosis` = Seq(DIAGNOSIS(`fhir_id` = "CD2", `icd_id_diagnosis` = "Q90.9")),
        `outcomes` = Seq(OUTCOME(`fhir_id` = "O2", `participant_fhir_id` = "P2")),
        `family` = FAMILY(`fhir_id` = "G1", `family_members_id` = Seq("P1", "P2", "P3"), `family_relations` = Seq(FAMILY_RELATIONS("P3", "son"))),
        `family_type` = "other"
      )
    )

    simple_participant.find(_.`fhir_id` === "P3") shouldBe Some(
      SIMPLE_PARTICIPANT(
        `fhir_id` = "P3",
        `phenotype` = null,
        `observed_phenotype` = null,
        `non_observed_phenotype` = null,
        `mondo` = null,
        `diagnosis` = null,
        `outcomes` = Nil,
        `family` = FAMILY(`fhir_id` = "G1", `family_members_id` = Seq("P1", "P2", "P3"), `family_relations` = Seq(FAMILY_RELATIONS("P2", "father"), FAMILY_RELATIONS("P1", "mother"))),
        `family_type` = "trio"
      )

    )
  }
  //Array(
  // SIMPLE_PARTICIPANT(P2,male,Not Reported,Not Reported,PAVKKD,PT_48DYT4PP,SD_Z6MWD3H0,re_000001,List(PHENOTYPE(CP2,Acute lymphoblastic leukemia (HP:0001631),null,true,0)),List(PHENOTYPE_ENRICHED(Abnormality of the cardiovascular system (HP:0001626),List(Phenotypic abnormality (HP:0000118)),false,false,List(0)), PHENOTYPE_ENRICHED(All (HP:0000001),List(),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal cardiac atrium morphology (HP:0005120),List(Abnormal heart morphology (HP:0001627)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal cardiac septum morphology (HP:0001671),List(Abnormal heart morphology (HP:0001627)),false,false,List(0)), PHENOTYPE_ENRICHED(Phenotypic abnormality (HP:0000118),List(All (HP:0000001)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal heart morphology (HP:0001627),List(Abnormality of cardiovascular system morphology (HP:0030680)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal atrial septum morphology (HP:0011994),List(Abnormal cardiac septum morphology (HP:0001671)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormality of cardiovascular system morphology (HP:0030680),List(Abnormality of the cardiovascular system (HP:0001626)),false,false,List(0)), PHENOTYPE_ENRICHED(Atrial septal defect (HP:0001631),List(Abnormal cardiac atrium morphology (HP:0005120), Abnormal atrial septum morphology (HP:0011994)),true,false,List(0))),null,List(DIAGNOSIS(CD2,DG_KG6TQWCT,Acute lymphoblastic leukemia,List(),List(),false,null,0,Q90.9,null,null)),null,List(OUTCOME(O2,P2,Alive,OC_ZKP9A89H,AGE_AT_EVENT(0,day,Birth))),LIGHT_STUDY_CENTRIC(42776,Kids First: Genomic Analysis of Congenital Heart Defects and Acute Lymphoblastic Leukemia in Children with Down Syndrome,completed,phs002330.v1.p1,phs002330,v1.p1,123456,SD_Z6MWD3H0,KF-CHDALL,Kids First,List(TODO),List(TODO),List(TODO),0,0,0,false),FAMILY(G1,FM_NV901ZZN,person,List(P1, P2, P3),List(FAMILY_RELATIONS(P3,son))),other,Other,TODO,false,111),
  // SIMPLE_PARTICIPANT(P2,male,Not Reported,Not Reported,PAVKKD,PT_48DYT4PP,SD_Z6MWD3H0,re_000001,List(PHENOTYPE(CP2,Acute lymphoblastic leukemia (HP_0001631),null,true,0)),List(PHENOTYPE_ENRICHED(Abnormality of the cardiovascular system (HP:0001626),List(Phenotypic abnormality (HP:0000118)),false,false,List(0)), PHENOTYPE_ENRICHED(All (HP:0000001),List(),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal cardiac atrium morphology (HP:0005120),List(Abnormal heart morphology (HP:0001627)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal cardiac septum morphology (HP:0001671),List(Abnormal heart morphology (HP:0001627)),false,false,List(0)), PHENOTYPE_ENRICHED(Phenotypic abnormality (HP:0000118),List(All (HP:0000001)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal heart morphology (HP:0001627),List(Abnormality of cardiovascular system morphology (HP:0030680)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal atrial septum morphology (HP:0011994),List(Abnormal cardiac septum morphology (HP:0001671)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormality of cardiovascular system morphology (HP:0030680),List(Abnormality of the cardiovascular system (HP:0001626)),false,false,List(0)), PHENOTYPE_ENRICHED(Atrial septal defect (HP:0001631),List(Abnormal cardiac atrium morphology (HP:0005120), Abnormal atrial septum morphology (HP:0011994)),true,false,List(0))),null,List(DIAGNOSIS(CD2,DG_KG6TQWCT,Acute lymphoblastic leukemia,List(),List(),false,null,0,Q90.9,null,null)),null,List(OUTCOME(O2,P2,Alive,OC_ZKP9A89H,AGE_AT_EVENT(0,day,Birth))),LIGHT_STUDY_CENTRIC(42776,Kids First: Genomic Analysis of Congenital Heart Defects and Acute Lymphoblastic Leukemia in Children with Down Syndrome,completed,phs002330.v1.p1,phs002330,v1.p1,123456,SD_Z6MWD3H0,KF-CHDALL,Kids First,List(TODO),List(TODO),List(TODO),0,0,0,false),FAMILY(G1,FM_NV901ZZN,person,List(P1, P2, P3),List(FAMILY_RELATIONS(P3,son))),other,Other,TODO,false,111),
  // SIMPLE_PARTICIPANT(P3,male,Not Reported,Not Reported,PAVKKD,PT_48DYT4PP,SD_Z6MWD3H0,re_000001,null,null,null,null,null,List(),LIGHT_STUDY_CENTRIC(42776,Kids First: Genomic Analysis of Congenital Heart Defects and Acute Lymphoblastic Leukemia in Children with Down Syndrome,completed,phs002330.v1.p1,phs002330,v1.p1,123456,SD_Z6MWD3H0,KF-CHDALL,Kids First,List(TODO),List(TODO),List(TODO),0,0,0,false),FAMILY(G1,FM_NV901ZZN,person,List(P1, P2, P3),List(FAMILY_RELATIONS(P2,father), FAMILY_RELATIONS(P1,mother))),trio,Other,TODO,false,111),
  // SIMPLE_PARTICIPANT(P3,male,Not Reported,Not Reported,PAVKKD,PT_48DYT4PP,SD_Z6MWD3H0,re_000001,List(),null,null,null,null,List(),LIGHT_STUDY_CENTRIC(42776,Kids First: Genomic Analysis of Congenital Heart Defects and Acute Lymphoblastic Leukemia in Children with Down Syndrome,completed,phs002330.v1.p1,phs002330,v1.p1,123456,SD_Z6MWD3H0,KF-CHDALL,Kids First,List(TODO),List(TODO),List(TODO),0,0,0,false),FAMILY(G1,FM_NV901ZZN,person,List(P1, P2, P3),List(FAMILY_RELATIONS(P2,father), FAMILY_RELATIONS(P1,mother))),trio,Other,TODO,false,111))

  //  SIMPLE_PARTICIPANT(P1,male,Not Reported,Not Reported,PAVKKD,PT_48DYT4PP,SD_Z6MWD3H0,re_000001,List(PHENOTYPE(CP1,Acute lymphoblastic leukemia (HP:0001631),null,true,0)),List(PHENOTYPE_ENRICHED(Abnormality of the cardiovascular system (HP:0001626),List(Phenotypic abnormality (HP:0000118)),false,false,List(0)), PHENOTYPE_ENRICHED(All (HP:0000001),List(),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal cardiac atrium morphology (HP:0005120),List(Abnormal heart morphology (HP:0001627)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal cardiac septum morphology (HP:0001671),List(Abnormal heart morphology (HP:0001627)),false,false,List(0)), PHENOTYPE_ENRICHED(Phenotypic abnormality (HP:0000118),List(All (HP:0000001)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal heart morphology (HP:0001627),List(Abnormality of cardiovascular system morphology (HP:0030680)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal atrial septum morphology (HP:0011994),List(Abnormal cardiac septum morphology (HP:0001671)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormality of cardiovascular system morphology (HP:0030680),List(Abnormality of the cardiovascular system (HP:0001626)),false,false,List(0)), PHENOTYPE_ENRICHED(Atrial septal defect (HP:0001631),List(Abnormal cardiac atrium morphology (HP:0005120), Abnormal atrial septum morphology (HP:0011994)),true,false,List(0))),null,List(DIAGNOSIS(CD1,DG_KG6TQWCT,Acute lymphoblastic leukemia,List(),List(),false,null,0,Q90.9,null,null)),null,List(OUTCOME(O1,P1,Alive,OC_ZKP9A89H,AGE_AT_EVENT(0,day,Birth))),LIGHT_STUDY_CENTRIC(42776,Kids First: Genomic Analysis of Congenital Heart Defects and Acute Lymphoblastic Leukemia in Children with Down Syndrome,completed,phs002330.v1.p1,phs002330,v1.p1,123456,SD_Z6MWD3H0,KF-CHDALL,Kids First,List(TODO),List(TODO),List(TODO),0,0,0,false),FAMILY(G1,FM_NV901ZZN,person,List(P1, P2, P3),List(FAMILY_RELATIONS(P3,son))),other,Other,TODO,false,111))
  //  SIMPLE_PARTICIPANT(P1,male,Not Reported,Not Reported,PAVKKD,PT_48DYT4PP,SD_Z6MWD3H0,re_000001,List(PHENOTYPE(CP1,Acute lymphoblastic leukemia (HP_0001631),null,true,0)),List(PHENOTYPE_ENRICHED(Abnormality of the cardiovascular system (HP:0001626),List(Phenotypic abnormality (HP:0000118)),false,false,List(0)), PHENOTYPE_ENRICHED(All (HP:0000001),List(),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal cardiac atrium morphology (HP:0005120),List(Abnormal heart morphology (HP:0001627)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal cardiac septum morphology (HP:0001671),List(Abnormal heart morphology (HP:0001627)),false,false,List(0)), PHENOTYPE_ENRICHED(Phenotypic abnormality (HP:0000118),List(All (HP:0000001)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal heart morphology (HP:0001627),List(Abnormality of cardiovascular system morphology (HP:0030680)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal atrial septum morphology (HP:0011994),List(Abnormal cardiac septum morphology (HP:0001671)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormality of cardiovascular system morphology (HP:0030680),List(Abnormality of the cardiovascular system (HP:0001626)),false,false,List(0)), PHENOTYPE_ENRICHED(Atrial septal defect (HP:0001631),List(Abnormal cardiac atrium morphology (HP:0005120), Abnormal atrial septum morphology (HP:0011994)),true,false,List(0))),null,List(DIAGNOSIS(CD1,DG_KG6TQWCT,Acute lymphoblastic leukemia,List(),List(),false,null,0,Q90.9,null,null)),null,List(OUTCOME(O1,P1,Alive,OC_ZKP9A89H,AGE_AT_EVENT(0,day,Birth))),LIGHT_STUDY_CENTRIC(42776,Kids First: Genomic Analysis of Congenital Heart Defects and Acute Lymphoblastic Leukemia in Children with Down Syndrome,completed,phs002330.v1.p1,phs002330,v1.p1,123456,SD_Z6MWD3H0,KF-CHDALL,Kids First,List(TODO),List(TODO),List(TODO),0,0,0,false),FAMILY(G1,FM_NV901ZZN,person,List(P1, P2, P3),List(FAMILY_RELATIONS(P3,son))),other,Other,TODO,false,111),
  //
  // List(
  // SIMPLE_PARTICIPANT(P1,male,Not Reported,Not Reported,PAVKKD,PT_48DYT4PP,SD_Z6MWD3H0,re_000001,List(PHENOTYPE(CP1,Acute lymphoblastic leukemia (HP_0001631),null,true,0)),List(PHENOTYPE_ENRICHED(Abnormality of the cardiovascular system (HP:0001626),List(Phenotypic abnormality (HP:0000118)),false,false,List(0)), PHENOTYPE_ENRICHED(All (HP:0000001),List(),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal cardiac atrium morphology (HP:0005120),List(Abnormal heart morphology (HP:0001627)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal cardiac septum morphology (HP:0001671),List(Abnormal heart morphology (HP:0001627)),false,false,List(0)), PHENOTYPE_ENRICHED(Phenotypic abnormality (HP:0000118),List(All (HP:0000001)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal heart morphology (HP:0001627),List(Abnormality of cardiovascular system morphology (HP:0030680)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal atrial septum morphology (HP:0011994),List(Abnormal cardiac septum morphology (HP:0001671)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormality of cardiovascular system morphology (HP:0030680),List(Abnormality of the cardiovascular system (HP:0001626)),false,false,List(0)), PHENOTYPE_ENRICHED(Atrial septal defect (HP:0001631),List(Abnormal cardiac atrium morphology (HP:0005120), Abnormal atrial septum morphology (HP:0011994)),true,false,List(0))),null,List(DIAGNOSIS(CD1,DG_KG6TQWCT,Acute lymphoblastic leukemia,List(),List(),false,null,0,Q90.9,null,null)),null,List(OUTCOME(O1,P1,Alive,OC_ZKP9A89H,AGE_AT_EVENT(0,day,Birth))),LIGHT_STUDY_CENTRIC(42776,Kids First: Genomic Analysis of Congenital Heart Defects and Acute Lymphoblastic Leukemia in Children with Down Syndrome,completed,phs002330.v1.p1,phs002330,v1.p1,123456,SD_Z6MWD3H0,KF-CHDALL,Kids First,List(TODO),List(TODO),List(TODO),0,0,0,false),FAMILY(G1,FM_NV901ZZN,person,List(P1, P2, P3),List(FAMILY_RELATIONS(P3,son))),other,Other,TODO,false,111),
  //  SIMPLE_PARTICIPANT(P2,male,Not Reported,Not Reported,PAVKKD,PT_48DYT4PP,SD_Z6MWD3H0,re_000001,List(PHENOTYPE(CP2,Acute lymphoblastic leukemia (HP_0001631),null,true,0)),List(PHENOTYPE_ENRICHED(Abnormality of the cardiovascular system (HP:0001626),List(Phenotypic abnormality (HP:0000118)),false,false,List(0)), PHENOTYPE_ENRICHED(All (HP:0000001),List(),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal cardiac atrium morphology (HP:0005120),List(Abnormal heart morphology (HP:0001627)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal cardiac septum morphology (HP:0001671),List(Abnormal heart morphology (HP:0001627)),false,false,List(0)), PHENOTYPE_ENRICHED(Phenotypic abnormality (HP:0000118),List(All (HP:0000001)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal heart morphology (HP:0001627),List(Abnormality of cardiovascular system morphology (HP:0030680)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormal atrial septum morphology (HP:0011994),List(Abnormal cardiac septum morphology (HP:0001671)),false,false,List(0)), PHENOTYPE_ENRICHED(Abnormality of cardiovascular system morphology (HP:0030680),List(Abnormality of the cardiovascular system (HP:0001626)),false,false,List(0)), PHENOTYPE_ENRICHED(Atrial septal defect (HP:0001631),List(Abnormal cardiac atrium morphology (HP:0005120), Abnormal atrial septum morphology (HP:0011994)),true,false,List(0))),null,List(DIAGNOSIS(CD2,DG_KG6TQWCT,Acute lymphoblastic leukemia,List(),List(),false,null,0,Q90.9,null,null)),null,List(OUTCOME(O2,P2,Alive,OC_ZKP9A89H,AGE_AT_EVENT(0,day,Birth))),LIGHT_STUDY_CENTRIC(42776,Kids First: Genomic Analysis of Congenital Heart Defects and Acute Lymphoblastic Leukemia in Children with Down Syndrome,completed,phs002330.v1.p1,phs002330,v1.p1,123456,SD_Z6MWD3H0,KF-CHDALL,Kids First,List(TODO),List(TODO),List(TODO),0,0,0,false),FAMILY(G1,FM_NV901ZZN,person,List(P1, P2, P3),List(FAMILY_RELATIONS(P3,son))),other,Other,TODO,false,111),
  //   SIMPLE_PARTICIPANT(P3,male,Not Reported,Not Reported,PAVKKD,PT_48DYT4PP,SD_Z6MWD3H0,re_000001,List(),null,null,null,null,List(),LIGHT_STUDY_CENTRIC(42776,Kids First: Genomic Analysis of Congenital Heart Defects and Acute Lymphoblastic Leukemia in Children with Down Syndrome,completed,phs002330.v1.p1,phs002330,v1.p1,123456,SD_Z6MWD3H0,KF-CHDALL,Kids First,List(TODO),List(TODO),List(TODO),0,0,0,false),FAMILY(G1,FM_NV901ZZN,person,List(P1, P2, P3),List(FAMILY_RELATIONS(P2,father), FAMILY_RELATIONS(P1,mother))),trio,Other,TODO,false,111))
}

