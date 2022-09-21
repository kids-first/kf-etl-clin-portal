import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.fhir.etl.common.Utils._
import model._
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{assert_true, col, collect_list, count, countDistinct, dense_rank, explode_outer, first, max, struct, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UtilsSpec extends AnyFlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  case class ConditionCoding(code: String, category: String)

  val SCHEMA_CONDITION_CODING = "array<struct<category:string,code:string>>"
  val allHpoTerms: DataFrame = read(getClass.getResource("/hpo_terms.json").toString, "Json", Map(), None, None)
  val allMondoTerms: DataFrame = read(getClass.getResource("/mondo_terms.json").toString, "Json", Map(), None, None)

  "addStudy" should "add studies to participant" in {
    val inputStudies = Seq(RESEARCHSTUDY()).toDF()
    val inputParticipants = Seq(PATIENT()).toDF()

    val output = inputParticipants.addStudy(inputStudies)

    output.collect().sameElements(Seq(PARTICIPANT_CENTRIC()))
  }

  "addOutcomes" should "add outcomes to participant" in {
    val inputPatients = Seq(
      PATIENT(`fhir_id` = "P1"),
      PATIENT(`fhir_id` = "P2")
    ).toDF()

    val inputObservationVitalStatus = Seq(
      OBSERVATION_VITAL_STATUS(`fhir_id` = "O1", `participant_fhir_id` = "P1"),
      OBSERVATION_VITAL_STATUS(`fhir_id` = "O3", `participant_fhir_id` = "P_NOT_THERE")
    ).toDF()

    val output = inputPatients.addOutcomes(inputObservationVitalStatus)

    val patientWithOutcome = output.select("fhir_id", "outcomes").as[(String, Seq[OUTCOME])].collect()

    val patient1 = patientWithOutcome.filter(_._1 == "P1").head
    val patient2 = patientWithOutcome.filter(_._1 == "P2").head
    patientWithOutcome.exists(_._1 == "P_NOT_THERE") shouldBe false

    patient1._2.map(_.`fhir_id`) shouldEqual Seq("O1")
    patient2._2.isEmpty shouldBe true
  }

  "addDownSyndromeDiagnosis" should "add down syndrome diagnosis to dataframe" in {
    val inputPatients = Seq(
      PATIENT(`fhir_id` = "P1"),
      PATIENT(`fhir_id` = "P2"),
      PATIENT(`fhir_id` = "P3")
    ).toDF()

    val mondoTerms = Seq(
      ("MONDO:0000000", "Another Term", Nil),
      ("MONDO:0008608", "Down Syndrome", Nil),
      ("MONDO:0008609", "Down Syndrome level 2", Seq("Down Syndrome (MONDO:0008608)"))
    ).toDF("id", "name", "parents")
    val inputDiseases = Seq(
      CONDITION_DISEASE(`fhir_id` = "O1", `participant_fhir_id` = "P1", `mondo_id` = Some("MONDO:0008608")),
      CONDITION_DISEASE(`fhir_id` = "O2", `participant_fhir_id` = "P1", `mondo_id` = Some("MONDO:0008609")),
      CONDITION_DISEASE(`fhir_id` = "O3", `participant_fhir_id` = "P2", `mondo_id` = Some("MONDO:0008609")),
      CONDITION_DISEASE(`fhir_id` = "O4", `participant_fhir_id` = "P2", `mondo_id` = Some("MONDO:0000000")),
      CONDITION_DISEASE(`fhir_id` = "O5", `participant_fhir_id` = "P3", `mondo_id` = Some("MONDO:0000000"))
    ).toDF()

    val output = inputPatients.addDownSyndromeDiagnosis(inputDiseases, mondoTerms)

    val patientWithDS = output.select("fhir_id", "down_syndrome_status", "down_syndrome_diagnosis").as[(String, String, Seq[String])].collect()

    patientWithDS.find(_._1 == "P1") shouldBe Some(
      ("P1", "T21", Seq("Down Syndrome (MONDO:0008608)", "Down Syndrome level 2 (MONDO:0008609)"))
    )
    patientWithDS.find(_._1 == "P2") shouldBe Some(
      ("P2", "T21", Seq("Down Syndrome level 2 (MONDO:0008609)"))
    )
    patientWithDS.find(_._1 == "P3") shouldBe Some(
      ("P3", "D21", null)
    )
  }

  "addFamily" should "add families to patients" in {
    val inputPatients = Seq(
      PATIENT(`fhir_id` = "FTH", `participant_id` = "P1"),
      PATIENT(`fhir_id` = "MTH", `participant_id` = "P2"),
      PATIENT(`fhir_id` = "SON", `participant_id` = "P3"),
      PATIENT(`fhir_id` = "44", `participant_id` = "P4")

    ).toDF()

    val inputFamilies = Seq(
      GROUP(`fhir_id` = "111", `family_id` = "FM_111", `family_members` = Seq(("FTH", false), ("MTH", false), ("SON", false)), `family_members_id` = Seq("FTH", "MTH", "SON")),
      GROUP(`fhir_id` = "222", `family_id` = "FM_222", `family_members` = Seq(("44", false)), `family_members_id` = Seq("44"))
    ).toDF()

    val inputFamilyRelationship = Seq(
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "MTH", `participant2_fhir_id` = "SON", `participant1_to_participant_2_relationship` = "father"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "FTH", `participant2_fhir_id` = "SON", `participant1_to_participant_2_relationship` = "mother"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "SON", `participant2_fhir_id` = "FTH", `participant1_to_participant_2_relationship` = "son"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "SON", `participant2_fhir_id` = "MTH", `participant1_to_participant_2_relationship` = "son"),
    ).toDF()

    val output = inputPatients.addFamily(inputFamilies, inputFamilyRelationship)
    val patientWithFamilies: Array[(String, FAMILY, String, String)] = output.select("fhir_id", "family", "family_type", "families_id").as[(String, FAMILY, String, String)].collect()

    patientWithFamilies.length shouldBe 4

    val patient3 = patientWithFamilies.filter(_._1 == "SON").head
    patient3._2.family_relations.map(_.`relation`) should contain theSameElementsAs Seq("mother", "father")
    patient3._2.mother_id shouldBe Some("P1")
    patient3._2.father_id shouldBe Some("P2")
    patient3._3 shouldBe "trio"
    patient3._4 shouldBe "FM_111"

    val patient1 = patientWithFamilies.filter(_._1 == "FTH").head
    patient1._2.family_relations.map(_.`relation`) shouldBe Seq("son")
    patient1._3 shouldBe "trio"
    patient1._4 shouldBe "FM_111"

    val patient2 = patientWithFamilies.filter(_._1 == "MTH").head
    patient2._3 shouldBe "trio"
    patient2._4 shouldBe "FM_111"

    val patient4 = patientWithFamilies.filter(_._1 == "44").head
    patient4._2 shouldBe null
    patient4._3 shouldBe "proband-only"
    patient4._4 shouldBe "FM_222"
  }

  it should "return duo" in {
    val inputPatients = Seq(
      PATIENT(`fhir_id` = "FTH", `participant_id` = "P1"),
      PATIENT(`fhir_id` = "SON", `participant_id` = "P2"),

    ).toDF()

    val inputFamilies = Seq(
      GROUP(`fhir_id` = "111", `family_id` = "FM_111", `family_members` = Seq(("FTH", false), ("SON", false)), `family_members_id` = Seq("FTH", "SON"))
    ).toDF()

    val inputFamilyRelationship = Seq(
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "FTH", `participant2_fhir_id` = "SON", `participant1_to_participant_2_relationship` = "father"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "SON", `participant2_fhir_id` = "FTH", `participant1_to_participant_2_relationship` = "son"),
    ).toDF()

    val output = inputPatients.addFamily(inputFamilies, inputFamilyRelationship)
    output.select("participant_id","family_type").as[ (String,String)].collect() should contain theSameElementsAs Seq(
      ("P1", "duo"),
      ("P2", "duo")
    )
  }

  it should "return duo+" in {
    val inputPatients = Seq(
      PATIENT(`fhir_id` = "FTH", `participant_id` = "P1"),
      PATIENT(`fhir_id` = "SON", `participant_id` = "P2"),
      PATIENT(`fhir_id` = "SON2", `participant_id` = "P3")

    ).toDF()

    val inputFamilies = Seq(
      GROUP(`fhir_id` = "111", `family_id` = "FM_111", `family_members` = Seq(("FTH", false), ("MTH", false), ("SON", false)), `family_members_id` = Seq("FTH", "SON2", "SON"))
    ).toDF()

    val inputFamilyRelationship = Seq(
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "FTH", `participant2_fhir_id` = "SON2", `participant1_to_participant_2_relationship` = "father"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "SON2", `participant2_fhir_id` = "FTH", `participant1_to_participant_2_relationship` = "son"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "SON", `participant2_fhir_id` = "FTH", `participant1_to_participant_2_relationship` = "son"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "FTH", `participant2_fhir_id` = "SON", `participant1_to_participant_2_relationship` = "father")
    ).toDF()

    val output = inputPatients.addFamily(inputFamilies, inputFamilyRelationship)
    output.select("participant_id","family_type").as[ (String,String)].collect() should contain theSameElementsAs Seq(
      ("P1", "duo+"),
      ("P2", "duo+"),
      ("P3", "duo+")
    )
  }

  it should "return duo+ if there is not a participant with a mother and a father" in {
    //Grand=Father -> Father -> Son = duo+
    val inputPatients = Seq(
      PATIENT(`fhir_id` = "FTH", `participant_id` = "P1"),
      PATIENT(`fhir_id` = "GFTH", `participant_id` = "P2"),
      PATIENT(`fhir_id` = "SON", `participant_id` = "P3")

    ).toDF()

    val inputFamilies = Seq(
      GROUP(`fhir_id` = "111", `family_id` = "FM_111", `family_members` = Seq(("FTH", false), ("GFTH", false), ("SON", false)), `family_members_id` = Seq("FTH", "GFTH", "SON"))
    ).toDF()

    val inputFamilyRelationship = Seq(
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "GFTH", `participant2_fhir_id` = "FTH", `participant1_to_participant_2_relationship` = "father"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "FTH", `participant2_fhir_id` = "SON", `participant1_to_participant_2_relationship` = "father"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "SON", `participant2_fhir_id` = "FTH", `participant1_to_participant_2_relationship` = "son"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "FTH", `participant2_fhir_id` = "GFTH", `participant1_to_participant_2_relationship` = "son")
    ).toDF()

    val output = inputPatients.addFamily(inputFamilies, inputFamilyRelationship)
    output.select("participant_id","family_type").as[ (String,String)].collect() should contain theSameElementsAs Seq(
      ("P1", "duo+"),
      ("P2", "duo+"),
      ("P3", "duo+")
    )
  }

  it should "return trio" in {
    val inputPatients = Seq(
      PATIENT(`fhir_id` = "FTH", `participant_id` = "P1"),
      PATIENT(`fhir_id` = "MTH", `participant_id` = "P2"),
      PATIENT(`fhir_id` = "SON", `participant_id` = "P3")

    ).toDF()

    val inputFamilies = Seq(
      GROUP(`fhir_id` = "111", `family_id` = "FM_111", `family_members` = Seq(("FTH", false), ("MTH", false), ("SON", false)), `family_members_id` = Seq("FTH", "MTH", "SON"))
    ).toDF()

    val inputFamilyRelationship = Seq(
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "MTH", `participant2_fhir_id` = "SON", `participant1_to_participant_2_relationship` = "father"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "FTH", `participant2_fhir_id` = "SON", `participant1_to_participant_2_relationship` = "mother"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "SON", `participant2_fhir_id` = "FTH", `participant1_to_participant_2_relationship` = "son"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "SON", `participant2_fhir_id` = "MTH", `participant1_to_participant_2_relationship` = "son")
    ).toDF()

    val output = inputPatients.addFamily(inputFamilies, inputFamilyRelationship)
    output.select("participant_id","family_type").as[ (String,String)].collect() should contain theSameElementsAs Seq(
      ("P1", "trio"),
      ("P2", "trio"),
      ("P3", "trio")
    )
  }

  it should "return trio+" in {
    val inputPatients = Seq(
      PATIENT(`fhir_id` = "FTH", `participant_id` = "P1"),
      PATIENT(`fhir_id` = "MTH", `participant_id` = "P2"),
      PATIENT(`fhir_id` = "SON", `participant_id` = "P3"),
      PATIENT(`fhir_id` = "SON2", `participant_id` = "P4")

    ).toDF()

    val inputFamilies = Seq(
      GROUP(`fhir_id` = "111", `family_id` = "FM_111", `family_members` = Seq(("FTH", false), ("MTH", false), ("SON", false), ("SON2", false)), `family_members_id` = Seq("FTH", "MTH", "SON", "SON2"))
    ).toDF()

    val inputFamilyRelationship = Seq(
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "MTH", `participant2_fhir_id` = "SON", `participant1_to_participant_2_relationship` = "father"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "FTH", `participant2_fhir_id` = "SON", `participant1_to_participant_2_relationship` = "mother"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "MTH", `participant2_fhir_id` = "SON2", `participant1_to_participant_2_relationship` = "father"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "FTH", `participant2_fhir_id` = "SON2", `participant1_to_participant_2_relationship` = "mother"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "SON", `participant2_fhir_id` = "FTH", `participant1_to_participant_2_relationship` = "son"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "SON", `participant2_fhir_id` = "MTH", `participant1_to_participant_2_relationship` = "son"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "SON2", `participant2_fhir_id` = "FTH", `participant1_to_participant_2_relationship` = "son"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "SON2", `participant2_fhir_id` = "MTH", `participant1_to_participant_2_relationship` = "son")
    ).toDF()

    val output = inputPatients.addFamily(inputFamilies, inputFamilyRelationship)
    output.select("participant_id","family_type").as[ (String,String)].collect() should contain theSameElementsAs Seq(
      ("P1", "trio+"),
      ("P2", "trio+"),
      ("P3", "trio+"),
      ("P4", "trio+")
    )
  }


  "addDiagnosisPhenotypes" should "group phenotypes by observed or non-observed" in {

    val inputParticipants = Seq(
      PATIENT(participant_id = "A", fhir_id = "A"),
      PATIENT(participant_id = "B", fhir_id = "B")
    ).toDF()

    val inputPhenotypes = Seq(
      CONDITION_PHENOTYPE(fhir_id = "1p", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0001631")), observed = "confirmed"),
      CONDITION_PHENOTYPE(fhir_id = "2p", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING())),
      CONDITION_PHENOTYPE(fhir_id = "3p", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING()), observed = "not"),
      CONDITION_PHENOTYPE(fhir_id = "4p", participant_fhir_id = "A")
    ).toDF()

    val inputDiseases = Seq.empty[CONDITION_DISEASE].toDF()

    val output = inputParticipants.addDiagnosisPhenotypes(inputPhenotypes, inputDiseases)(allHpoTerms, allMondoTerms)
    val participantPhenotypes = output.select("participant_id", "phenotype").as[(String, Seq[PHENOTYPE])].collect()

    val participantA_Ph = participantPhenotypes.filter(_._1 == "A").head
    participantA_Ph._2.map(p => (p.fhir_id, p.`is_observed`)) should contain theSameElementsAs Seq(("1p", true), ("2p", false), ("3p", false))

    val participantB_Ph = participantPhenotypes.find(_._1 == "B")
    participantB_Ph shouldBe Some(("B", null))
  }

  it should "map diseases to participants" in {
    val inputParticipants = Seq(
      PATIENT(participant_id = "A", fhir_id = "A"),
      PATIENT(participant_id = "B", fhir_id = "B")
    ).toDF()

    val inputPhenotypes = Seq.empty[CONDITION_PHENOTYPE].toDF()

    val inputDiseases = Seq(
      CONDITION_DISEASE(fhir_id = "1d", diagnosis_id = "diag1", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING(`category` = "ICD", `code` = "Q90.9"))),
      CONDITION_DISEASE(fhir_id = "2d", diagnosis_id = "diag2", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING(`category` = "NCIT", `code` = "Some NCIT"))),
      CONDITION_DISEASE(fhir_id = "3d", diagnosis_id = "diag3", participant_fhir_id = "A")
    ).toDF()

    val output = inputParticipants.addDiagnosisPhenotypes(inputPhenotypes, inputDiseases)(allHpoTerms, allMondoTerms)

    val participantDiseases =
      output
        .select("participant_id", "diagnosis")
        .withColumn("diagnosis_exp", explode_outer(col("diagnosis")))
        .select("participant_id", "diagnosis_exp.diagnosis_id")
        .as[(String, String)].collect()

    val participantA_D = participantDiseases.filter(_._1 == "A")
    val participantB_D = participantDiseases.filter(_._1 == "B").head

    participantA_D.map(_._2) should contain theSameElementsAs Seq("diag1", "diag2")
    participantB_D._2 shouldBe null
  }

  it should "generate observed_phenotypes and non_observed_phenotypes" in {
    val inputParticipants = Seq(
      PATIENT(participant_id = "A", fhir_id = "A")
    ).toDF()

    val inputPhenotypes = Seq(
      CONDITION_PHENOTYPE(
        fhir_id = "1p",
        participant_fhir_id = "A",
        condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0000234")),
        observed = "confirmed"
      ),
      CONDITION_PHENOTYPE(
        fhir_id = "2p",
        participant_fhir_id = "A",
        condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0033127")),
        observed = "not",
        age_at_event = AGE_AT_EVENT(5)
      ),
      CONDITION_PHENOTYPE(
        fhir_id = "3p",
        participant_fhir_id = "A",
        condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0002086")),
        age_at_event = AGE_AT_EVENT(10)
      )
    ).toDF()

    val inputDiseases = Seq.empty[CONDITION_DISEASE].toDF()

    val output =
      inputParticipants
        .addDiagnosisPhenotypes(inputPhenotypes, inputDiseases)(allHpoTerms, allMondoTerms)
        .select("participant_id", "observed_phenotype", "non_observed_phenotype")
        .as[(String, Seq[OBSERVABLE_TERM], Seq[OBSERVABLE_TERM])].collect()


    val (_, observedPheno, nonObservedPheno) = output.filter(_._1 == "A").head

    observedPheno.count(_.`is_tagged`) shouldBe 1
    assert(observedPheno.forall(_.`age_at_event_days` == Seq(0)))

    nonObservedPheno.count(_.`is_tagged`) shouldBe 2
    nonObservedPheno.flatMap(_.`age_at_event_days`).distinct should contain only(5, 10)
  }

  it should "group diagnosis by age at event days" in {
    val inputParticipants = Seq(
      PATIENT(participant_id = "A", fhir_id = "A")
    ).toDF()

    val inputPhenotypes = Seq.empty[CONDITION_PHENOTYPE].toDF()

    val inputDiseases = Seq(
      CONDITION_DISEASE(
        fhir_id = "1d",
        diagnosis_id = "diag1",
        participant_fhir_id = "A",
        condition_coding = Seq(CONDITION_CODING(`category` = "MONDO", `code` = "MONDO_0002051")),
        mondo_id = Some("MONDO:0002051"),
        age_at_event = AGE_AT_EVENT(5),
      ),
      CONDITION_DISEASE(
        fhir_id = "2d",
        diagnosis_id = "diag2",
        participant_fhir_id = "A",
        condition_coding = Seq(CONDITION_CODING(`category` = "MONDO", `code` = "MONDO_0024458")),
        mondo_id = Some("MONDO:0024458"),
        age_at_event = AGE_AT_EVENT(10),
      ),
      CONDITION_DISEASE(fhir_id = "3d", diagnosis_id = "diag3", participant_fhir_id = "A")
    ).toDF()

    val output =
      inputParticipants
        .addDiagnosisPhenotypes(inputPhenotypes, inputDiseases)(allHpoTerms, allMondoTerms)
        .select("participant_id", "mondo")
        .as[(String, Seq[OBSERVABLE_TERM])].collect()

    val participantA_Ph = output.filter(_._1 == "A").head

    participantA_Ph._2.filter(t => t.`name` === "disease or disorder (MONDO:0000001)").head.`age_at_event_days` shouldEqual Seq(5, 10)
  }

  "addBiospecimenParticipant" should "add participant - only one" in {
    val inputBiospecimen = Seq(
      BIOSPECIMEN(`participant_fhir_id` = "A", `fhir_id` = "1"),
      BIOSPECIMEN(`participant_fhir_id` = "B", `fhir_id` = "2"),
      BIOSPECIMEN(`participant_fhir_id` = "C", `fhir_id` = "3")
    ).toDF()

    val inputParticipant = Seq(
      SIMPLE_PARTICIPANT(`fhir_id` = "A", participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "A", participant_fhir_id_2 = "A"), `participant_id` = "P_A"),
      SIMPLE_PARTICIPANT(`fhir_id` = "B", participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "B", participant_fhir_id_2 = "B"),`participant_id` = "P_B")
    ).toDF()

    val output = inputBiospecimen.addBiospecimenParticipant(inputParticipant)

    val biospecimenWithParticipant = output.select("fhir_id", "participant").as[(String, SIMPLE_PARTICIPANT)].collect()
    val biospecimen1 = biospecimenWithParticipant.filter(_._1 == "1").head
    val biospecimen2 = biospecimenWithParticipant.filter(_._1 == "2").head

    biospecimen1._2.`participant_id` shouldEqual "P_A"
    biospecimen2._2.`participant_id` shouldEqual "P_B"

    // Ignore biospecimen without participant
    biospecimenWithParticipant.exists(_._1 == "3") shouldEqual false
  }

  "addParticipantFilesWithBiospecimen" should "add files with their biospecimen for a specific participant" in {
    // Input data

    // F1 -> B11, B12, B21
    // F2 -> B11, B13, B31, B32
    // F3 -> B22
    // F4 -> B33
    // F5 -> No biospecimen

    // P1 -> B11, B12, B13
    // P2 -> B21, B22
    // P3 -> B31, B32, B33
    // P4 -> No biospecimen
    // P5 -> No file

    // F6, F7 and B_NOT_THERE1 are related to a missing participant (should be ignored)

    val inputParticipant = Seq(
      SIMPLE_PARTICIPANT(`fhir_id` = "P1"),
      SIMPLE_PARTICIPANT(`fhir_id` = "P2"),
      SIMPLE_PARTICIPANT(`fhir_id` = "P3"),
      SIMPLE_PARTICIPANT(`fhir_id` = "P4"),
      SIMPLE_PARTICIPANT(`fhir_id` = "P5")
    ).toDF()

    val inputBiospecimen = Seq(
      BIOSPECIMEN_INPUT(`participant_fhir_id` = "P1", `fhir_id` = "B11"),
      BIOSPECIMEN_INPUT(`participant_fhir_id` = "P1", `fhir_id` = "B12"),
      BIOSPECIMEN_INPUT(`participant_fhir_id` = "P1", `fhir_id` = "B13"),
      BIOSPECIMEN_INPUT(`participant_fhir_id` = "P2", `fhir_id` = "B21"),
      BIOSPECIMEN_INPUT(`participant_fhir_id` = "P2", `fhir_id` = "B22"),
      BIOSPECIMEN_INPUT(`participant_fhir_id` = "P3", `fhir_id` = "B31"),
      BIOSPECIMEN_INPUT(`participant_fhir_id` = "P3", `fhir_id` = "B32"),
      BIOSPECIMEN_INPUT(`participant_fhir_id` = "P3", `fhir_id` = "B33"),

      BIOSPECIMEN_INPUT(`participant_fhir_id` = "P_NOT_THERE", `fhir_id` = "B_NOT_THERE1")
    ).toDF()

    val inputDocumentReference = Seq(
      DOCUMENTREFERENCE(`participant_fhir_id` = null, `fhir_id` = "F1", `specimen_fhir_ids` = Seq("B11", "B12", "B21")),
      DOCUMENTREFERENCE(`participant_fhir_id` = "P1", `fhir_id` = "F2", `specimen_fhir_ids` = Seq("B11", "B13", "B31", "B32")),
      DOCUMENTREFERENCE(`participant_fhir_id` = "P2", `fhir_id` = "F3", `specimen_fhir_ids` = Seq("B22")),
      DOCUMENTREFERENCE(`participant_fhir_id` = "P3", `fhir_id` = "F4", `specimen_fhir_ids` = Seq("B33")),
      DOCUMENTREFERENCE(`participant_fhir_id` = "P2", `fhir_id` = "F5", `specimen_fhir_ids` = Seq.empty),

      DOCUMENTREFERENCE(`participant_fhir_id` = "P_NOT_THERE", `fhir_id` = "F6", `specimen_fhir_ids` = Seq("B_NOT_THERE1")),
      DOCUMENTREFERENCE(`participant_fhir_id` = "P_NOT_THERE", `fhir_id` = "F7", `specimen_fhir_ids` = Seq.empty),
    ).toDF()

    val output = inputParticipant.addParticipantFilesWithBiospecimen(inputDocumentReference, inputBiospecimen)

    val participantWithFileAndSpecimen = output.select("fhir_id", "files").as[(String, Seq[FILE_WITH_BIOSPECIMEN])].collect()

    // Assertions
    // P1 -> F1 -> B11 & B12
    // P1 -> F2 -> B11 & B13
    // P2 -> F1 -> B21
    // P2 -> F3 -> B22
    // P2 -> F5
    // P3 -> F2 -> B31 & B32
    // P3 -> F4 -> B33
    // P3 -> F5
    // P4 -> F5
    // P5 -> No file
    // P_NOT_THERE should not be there

    val participant1 = participantWithFileAndSpecimen.filter(_._1 == "P1").head
    val participant2 = participantWithFileAndSpecimen.filter(_._1 == "P2").head
    val participant3 = participantWithFileAndSpecimen.filter(_._1 == "P3").head
    val participant4 = participantWithFileAndSpecimen.filter(_._1 == "P4").head
    val participant5 = participantWithFileAndSpecimen.filter(_._1 == "P5").head

    participantWithFileAndSpecimen.exists(_._1 == "P_NOT_THERE") shouldBe false

    participant1._2.map(_.`fhir_id`) == Seq("F1", "F2")
    participant2._2.map(_.`fhir_id`) == Seq("F1", "F3", "F5")
    participant3._2.map(_.`fhir_id`) == Seq("F2", "F4", "F5")
    participant4._2.map(_.`fhir_id`) == Seq("F5")
    participant5._2.isEmpty shouldBe true

    val participantP1FileF1 = participant1._2.filter(_.`fhir_id`.contains("F1")).head
    participantP1FileF1.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B11", "B12")

    val participantP1FileF2 = participant1._2.filter(_.`fhir_id`.contains("F2")).head
    participantP1FileF2.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B11", "B13")

    val participantP2FileF1 = participant2._2.filter(_.`fhir_id`.contains("F1")).head
    participantP2FileF1.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B21")

    val participantP2FileF3 = participant2._2.filter(_.`fhir_id`.contains("F3")).head
    participantP2FileF3.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B22")

    val participantP2FileF5 = participant2._2.filter(_.`fhir_id`.contains("F5")).head
    participantP2FileF5.`biospecimens`.isEmpty shouldBe true

    val participantP3FileF2 = participant3._2.filter(_.`fhir_id`.contains("F2")).head
    participantP3FileF2.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B31", "B32")

    val participantP3FileF4 = participant3._2.filter(_.`fhir_id`.contains("F4")).head
    participantP3FileF4.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B33")

  }

  it should "add empty files for biospecimen without files for a specific participant" in {
    val inputParticipant = Seq(
      SIMPLE_PARTICIPANT(`fhir_id` = "P1"),
    ).toDF()

    val inputBiospecimen = Seq(
      BIOSPECIMEN_INPUT(`participant_fhir_id` = "P1", `fhir_id` = "B11"),
      BIOSPECIMEN_INPUT(`participant_fhir_id` = "P1", `fhir_id` = "B12"), //No file associated
    ).toDF()

    val inputDocumentReference = Seq(
      DOCUMENTREFERENCE(`participant_fhir_id` = "P1", `fhir_id` = "F1", `specimen_fhir_ids` = Seq("B11")),
    ).toDF()

    val output = inputParticipant.addParticipantFilesWithBiospecimen(inputDocumentReference, inputBiospecimen)

    //B11 and B12 should be attached to P1
    val participant1AndSpecimen = output.select("fhir_id", "files.biospecimens").filter(col("fhir_id") === "P1").as[(String, Seq[Seq[BIOSPECIMEN]])].collect()
    participant1AndSpecimen.head._2.flatten.map(_.fhir_id) should contain theSameElementsAs Seq("B11", "B12")

    //P1 should contain one file and one dummy file
    val participantWithFile = output.select("fhir_id", "files.file_name").filter(col("fhir_id") === "P1").as[(String, Seq[String])].collect()
    participantWithFile.head._2 should contain theSameElementsAs Seq("4db9adf4-94f7-4800-a360-49eda89dfb62.g.vcf.gz", "dummy_file")
  }


}
