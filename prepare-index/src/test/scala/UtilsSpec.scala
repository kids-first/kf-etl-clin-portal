import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.fhir.etl.common.Utils._
import model._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{assert_true, col, explode_outer}
import org.scalatest.{FlatSpec, Matchers}

class UtilsSpec extends FlatSpec with Matchers with WithSparkSession {

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

  "addFamily" should "add families to patients" in {
    val inputPatients = Seq(
      PATIENT(`fhir_id` = "11"),
      PATIENT(`fhir_id` = "22"),
      PATIENT(`fhir_id` = "33"),
      PATIENT(`fhir_id` = "44")

    ).toDF()

    val inputFamilies = Seq(
      GROUP(`fhir_id` = "111", `family_id` = "FM_111", `family_members` = Seq(("11", false), ("22", false), ("33", false)), `family_members_id` = Seq("11", "22", "33")),
      GROUP(`fhir_id` = "222", `family_id` = "FM_222", `family_members` = Seq(("44", false)), `family_members_id` = Seq("44"))
    ).toDF()

    val inputFamilyRelationship = Seq(
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "22", `participant2_fhir_id` = "33", `participant1_to_participant_2_relationship` = "father"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "11", `participant2_fhir_id` = "33", `participant1_to_participant_2_relationship` = "mother"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "33", `participant2_fhir_id` = "11", `participant1_to_participant_2_relationship` = "son"),
      FAMILY_RELATIONSHIP(`participant1_fhir_id` = "33", `participant2_fhir_id` = "22", `participant1_to_participant_2_relationship` = "son"),
    ).toDF()

    val output = inputPatients.addFamily(inputFamilies, inputFamilyRelationship)
    val patientWithFamilies = output.select("fhir_id", "family").as[(String, FAMILY)].collect()

    val patient3 = patientWithFamilies.filter(_._1 == "33").head
    patient3._2.family_relations.map(_.`relation`) should contain theSameElementsAs Seq("mother", "father")

    val patient1 = patientWithFamilies.filter(_._1 == "11").head
    patient1._2.family_relations.map(_.`relation`) shouldBe Seq("son")

    val patient4 = patientWithFamilies.filter(_._1 == "44").head
    patient4._2 shouldBe null
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
        age_at_event = AGE_AT_EVENT(5),
      ),
      CONDITION_DISEASE(
        fhir_id = "2d",
        diagnosis_id = "diag2",
        participant_fhir_id = "A",
        condition_coding = Seq(CONDITION_CODING(`category` = "MONDO", `code` = "MONDO_0024458")),
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
      SIMPLE_PARTICIPANT(`fhir_id` = "A", `participant_id` = "P_A"),
      SIMPLE_PARTICIPANT(`fhir_id` = "B", `participant_id` = "P_B")
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
      BIOSPECIMEN(`participant_fhir_id` = "P1", `fhir_id` = "B11"),
      BIOSPECIMEN(`participant_fhir_id` = "P1", `fhir_id` = "B12"),
      BIOSPECIMEN(`participant_fhir_id` = "P1", `fhir_id` = "B13"),
      BIOSPECIMEN(`participant_fhir_id` = "P2", `fhir_id` = "B21"),
      BIOSPECIMEN(`participant_fhir_id` = "P2", `fhir_id` = "B22"),
      BIOSPECIMEN(`participant_fhir_id` = "P3", `fhir_id` = "B31"),
      BIOSPECIMEN(`participant_fhir_id` = "P3", `fhir_id` = "B32"),
      BIOSPECIMEN(`participant_fhir_id` = "P3", `fhir_id` = "B33"),

      BIOSPECIMEN(`participant_fhir_id` = "P_NOT_THERE", `fhir_id` = "B_NOT_THERE1")
    ).toDF()

    val inputDocumentReference = Seq(
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P1", "P2"), `fhir_id` = "F1", `specimen_fhir_ids` = Seq("B11", "B12", "B21")),
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P1", "P3"), `fhir_id` = "F2", `specimen_fhir_ids` = Seq("B11", "B13", "B31", "B32")),
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P2"), `fhir_id` = "F3", `specimen_fhir_ids` = Seq("B22")),
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P3"), `fhir_id` = "F4", `specimen_fhir_ids` = Seq("B33")),
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P2", "P3", "P4"), `fhir_id` = "F5", `specimen_fhir_ids` = Seq.empty),

      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P_NOT_THERE"), `fhir_id` = "F6", `specimen_fhir_ids` = Seq("B_NOT_THERE1")),
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P_NOT_THERE"), `fhir_id` = "F7", `specimen_fhir_ids` = Seq.empty),
    ).toDF()

    val inputSequencingExperiments = Seq(
      TASK()
    ).toDF()

    val output = inputParticipant.addParticipantFilesWithBiospecimen(inputDocumentReference, inputBiospecimen, inputSequencingExperiments)

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

    val participantP1FileF1 = participant1._2.filter(_.`fhir_id` == "F1").head
    participantP1FileF1.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B11", "B12")

    val participantP1FileF2 = participant1._2.filter(_.`fhir_id` == "F2").head
    participantP1FileF2.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B11", "B13")

    val participantP2FileF1 = participant2._2.filter(_.`fhir_id` == "F1").head
    participantP2FileF1.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B21")

    val participantP2FileF3 = participant2._2.filter(_.`fhir_id` == "F3").head
    participantP2FileF3.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B22")

    val participantP2FileF5 = participant2._2.filter(_.`fhir_id` == "F5").head
    participantP2FileF5.`biospecimens`.isEmpty shouldBe true

    val participantP3FileF2 = participant3._2.filter(_.`fhir_id` == "F2").head
    participantP3FileF2.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B31", "B32")

    val participantP3FileF4 = participant3._2.filter(_.`fhir_id` == "F4").head
    participantP3FileF4.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B33")

    val participantP3FileF5 = participant3._2.filter(_.`fhir_id` == "F5").head
    participantP3FileF5.`biospecimens`.isEmpty shouldBe true
  }

  it should "add empty files for biospecimen without files for a specific participant" in {
    val inputParticipant = Seq(
      SIMPLE_PARTICIPANT(`fhir_id` = "P1"),
    ).toDF()

    val inputBiospecimen = Seq(
      BIOSPECIMEN(`participant_fhir_id` = "P1", `fhir_id` = "B11"),
      BIOSPECIMEN(`participant_fhir_id` = "P1", `fhir_id` = "B12"), //No file associated
    ).toDF()

    val inputDocumentReference = Seq(
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P1"), `fhir_id` = "F1", `specimen_fhir_ids` = Seq("B11")),
    ).toDF()

    val inputSequencingExperiments = Seq.empty[TASK].toDF()

    val output = inputParticipant.addParticipantFilesWithBiospecimen(inputDocumentReference, inputBiospecimen, inputSequencingExperiments)

    //B11 and B12 should be attached to P1
    val participant1AndSpecimen = output.select("fhir_id","files.biospecimens").filter(col("fhir_id") === "P1").as[(String, Seq[Seq[BIOSPECIMEN]])].collect()
    participant1AndSpecimen.head._2.flatten.map(_.fhir_id) should contain theSameElementsAs Seq("B11", "B12")

    //P1 should contain one file and one dummy file
    val participantWithFile = output.select("fhir_id","files.file_name").filter(col("fhir_id") === "P1").as[(String, Seq[String])].collect()
    participantWithFile.head._2 should contain theSameElementsAs Seq("4db9adf4-94f7-4800-a360-49eda89dfb62.g.vcf.gz", "dummy_file")
  }

  "addFileParticipantsWithBiospecimen" should "add participant with their biospecimen for a specific file" in {
    // Input data

    // F1 -> B11, B12, B21
    // F2 -> B11, B13, B31, B32
    // F3 -> B22
    // F4 -> B33
    // F5 -> No biospecimen (and no participant -> should be ignored)

    // P1 -> B11, B12, B13
    // P2 -> B21, B22
    // P3 -> B31, B32, B33
    // P4 -> No biospecimen but P4 -> F4
    // P5 -> No file

    // F6, F7 and B_NOT_THERE1 are related to a missing participant

    val inputParticipant = Seq(
      SIMPLE_PARTICIPANT(`fhir_id` = "P1"),
      SIMPLE_PARTICIPANT(`fhir_id` = "P2"),
      SIMPLE_PARTICIPANT(`fhir_id` = "P3"),
      SIMPLE_PARTICIPANT(`fhir_id` = "P4"),
      SIMPLE_PARTICIPANT(`fhir_id` = "P5")
    ).toDF()

    val inputBiospecimen = Seq(
      BIOSPECIMEN(`participant_fhir_id` = "P1", `fhir_id` = "B11"),
      BIOSPECIMEN(`participant_fhir_id` = "P1", `fhir_id` = "B12"),
      BIOSPECIMEN(`participant_fhir_id` = "P1", `fhir_id` = "B13"),
      BIOSPECIMEN(`participant_fhir_id` = "P2", `fhir_id` = "B21"),
      BIOSPECIMEN(`participant_fhir_id` = "P2", `fhir_id` = "B22"),
      BIOSPECIMEN(`participant_fhir_id` = "P3", `fhir_id` = "B31"),
      BIOSPECIMEN(`participant_fhir_id` = "P3", `fhir_id` = "B32"),
      BIOSPECIMEN(`participant_fhir_id` = "P3", `fhir_id` = "B33"),

      BIOSPECIMEN(`participant_fhir_id` = "P_NOT_THERE", `fhir_id` = "B_NOT_THERE1")
    ).toDF()

    val inputDocumentReference = Seq(
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P1", "P2"), `fhir_id` = "F1", `specimen_fhir_ids` = Seq("B11", "B12", "B21")),
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P1", "P3"), `fhir_id` = "F2", `specimen_fhir_ids` = Seq("B11", "B13", "B31", "B32")),
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P2"), `fhir_id` = "F3", `specimen_fhir_ids` = Seq("B22")),
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P3", "P4"), `fhir_id` = "F4", `specimen_fhir_ids` = Seq("B33")),
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq.empty, `fhir_id` = "F5", `specimen_fhir_ids` = Seq.empty),

      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P_NOT_THERE"), `fhir_id` = "F6", `specimen_fhir_ids` = Seq("B_NOT_THERE1")),
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P_NOT_THERE"), `fhir_id` = "F7", `specimen_fhir_ids` = Seq.empty),
    ).toDF()

    val inputSequencingExperiments = Seq(
      TASK(`fhir_id` = "1", `document_reference_fhir_ids` = Seq("F1")),
      TASK(`fhir_id` = "2", `document_reference_fhir_ids` = Seq("F2", "F3")),
      TASK(`fhir_id` = "3", `document_reference_fhir_ids` = Seq("F5")),
    ).toDF()

    val output = inputDocumentReference.addFileParticipantsWithBiospecimen(inputParticipant, inputBiospecimen, inputSequencingExperiments)

    val fileWithParticipantAndSpecimen = output.select("fhir_id", "participants").as[(String, Seq[PARTICIPANT_WITH_BIOSPECIMEN])].collect()

    // Assertions
    // F1 -> P1 -> B11 & B12
    // F1 -> P2 -> B21
    // F2 -> P1 -> B11 & B13
    // F2 -> P3 -> B31 & B32
    // F3 -> P2 -> B22
    // F4 -> P3 -> B33
    // F4 -> P4 -> No biospecimen
    // F5, F6 and F7 should not be there

    val file1 = fileWithParticipantAndSpecimen.filter(_._1 == "F1").head
    val file2 = fileWithParticipantAndSpecimen.filter(_._1 == "F2").head
    val file3 = fileWithParticipantAndSpecimen.filter(_._1 == "F3").head
    val file4 = fileWithParticipantAndSpecimen.filter(_._1 == "F4").head

    fileWithParticipantAndSpecimen.exists(_._1 == "F5") shouldBe false
    fileWithParticipantAndSpecimen.exists(_._1 == "F6") shouldBe false
    fileWithParticipantAndSpecimen.exists(_._1 == "F7") shouldBe false

    file1._2.map(_.`fhir_id`) == Seq("P1", "P2")
    file2._2.map(_.`fhir_id`) == Seq("P1", "P3")
    file3._2.map(_.`fhir_id`) == Seq("P2")
    file4._2.map(_.`fhir_id`) == Seq("P3", "P4")

    val fileF1ParticipantP1 = file1._2.filter(_.`fhir_id` == "P1").head
    fileF1ParticipantP1.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B11", "B12")

    val fileF1ParticipantP2 = file1._2.filter(_.`fhir_id` == "P2").head
    fileF1ParticipantP2.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B21")

    val fileF2ParticipantP1 = file2._2.filter(_.`fhir_id` == "P1").head
    fileF2ParticipantP1.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B11", "B13")

    val fileF2ParticipantP3 = file2._2.filter(_.`fhir_id` == "P3").head
    fileF2ParticipantP3.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B31", "B32")

    val fileF3ParticipantP2 = file3._2.filter(_.`fhir_id` == "P2").head
    fileF3ParticipantP2.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B22")

    val fileF4ParticipantP3 = file4._2.filter(_.`fhir_id` == "P3").head
    fileF4ParticipantP3.`biospecimens`.map(_.`fhir_id`) should contain theSameElementsAs Seq("B33")

    val fileF4ParticipantP4 = file4._2.filter(_.`fhir_id` == "P4").head
    fileF4ParticipantP4.`biospecimens`.isEmpty shouldBe true
  }

  "addBiospecimenFiles" should "add files to biospecimem" in {
    // Input data

    // F1 -> B1
    // F2 -> B1, B2
    // F3 -> B_NOT_THERE (missing biospecimen, should be ignored)

    // B3 is not linked to any file

    val inputBiospecimen = Seq(
      BIOSPECIMEN(`participant_fhir_id` = "P1", `fhir_id` = "B1"),
      BIOSPECIMEN(`participant_fhir_id` = "P2", `fhir_id` = "B2"),
      BIOSPECIMEN(`participant_fhir_id` = "P3", `fhir_id` = "B3"),
    ).toDF()

    val inputDocumentReference = Seq(
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P1"), `fhir_id` = "F1", `specimen_fhir_ids` = Seq("B1")),
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P1", "P2"), `fhir_id` = "F2", `specimen_fhir_ids` = Seq("B1", "B2")),
      DOCUMENTREFERENCE(`participant_fhir_ids` = Seq("P3"), `fhir_id` = "F3", `specimen_fhir_ids` = Seq("B_NOT_THERE")),
    ).toDF()

    val inputSequenceExperiments = Seq(
      TASK(`fhir_id` = "T1", `document_reference_fhir_ids` = Seq("F1")),
      TASK(`fhir_id` = "T2", `document_reference_fhir_ids` = Seq("F2"))
    ).toDF()

    val output = inputBiospecimen.addBiospecimenFiles(inputDocumentReference, inputSequenceExperiments)

    val biospecimenWithFiles = output.select("fhir_id", "files").as[(String, Seq[DOCUMENTREFERENCE_WITH_SEQ_EXP])].collect()

    // Assertions
    // B1 -> F1 & F2
    // B2 -> F2
    // B3 -> no file
    // B_NOT_THERE is not in result

    val biospecimen1 = biospecimenWithFiles.filter(_._1 == "B1").head
    biospecimen1._2.map(_.`fhir_id`) should contain theSameElementsAs Seq("F1", "F2")

    val biospecimen2 = biospecimenWithFiles.filter(_._1 == "B2").head
    biospecimen2._2.map(_.`fhir_id`) should contain theSameElementsAs Seq("F2")

    val biospecimen3 = biospecimenWithFiles.filter(_._1 == "B3").head
    biospecimen3._2.isEmpty shouldBe true

    biospecimenWithFiles.exists(_._1 == "B_NOT_THERE") shouldBe false
  }

}
