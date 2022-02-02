import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.fhir.etl.common.Utils._
import model._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode_outer}
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

  "addBiospecimen" should "add biospecimen to participant" in {
    val inputParticipants = Seq(
      PATIENT(participant_id = "A", fhir_id = "A"),
      PATIENT(participant_id = "B", fhir_id = "B")
    ).toDF()

    val inputBiospecimens = Seq(
      BIOSPECIMEN(fhir_id = "1", participant_fhir_id = "A"),
      BIOSPECIMEN(fhir_id = "2", participant_fhir_id = "A"),
      BIOSPECIMEN(fhir_id = "3", participant_fhir_id = "C")
    ).toDF()


    val output = inputParticipants.addBiospecimen(inputBiospecimens)
    val participantBio = output.select("participant_id", "biospecimens.fhir_id").map(r => r.getString(0) -> r.getSeq[String](1)).collect()
    val participantA = participantBio.filter(_._1 == "A").head
    val participantB = participantBio.filter(_._1 == "B").head

    participantA._2 shouldEqual Seq("1", "2")
    participantB._2 shouldEqual Seq.empty[String]
  }

  "addFamily" should "add families to participant" in {

    val participantWith1Family = PATIENT(`fhir_id` = "11")
    val participantWith2Families = PATIENT(`fhir_id` = "22")
    val participantWithNoFamily = PATIENT(`fhir_id` = "33")

    val family1 = FAMILY(`fhir_id` = "111", `family_id` = "FM_111", `family_members` = Seq(("11", false), ("22", false)), `family_members_id` = Seq("11", "22"))
    val family2 = FAMILY(`fhir_id` = "222", `family_id` = "FM_222", `family_members` = Seq(("22", false)), `family_members_id` = Seq("22"))

    val inputFamilies = Seq(family1, family2).toDF()
    val inputParticipants = Seq(participantWith1Family, participantWith2Families, participantWithNoFamily).toDF()

    val participantCentrics = inputParticipants.addFamily(inputFamilies)

    participantCentrics.count shouldEqual 3

    val participantCentricWith1Family = participantCentrics.as[PATIENT_WITH_FAMILY].collect().filter(p => p.`fhir_id`.equals("11")).head
    val participantCentricWith2Families = participantCentrics.as[PATIENT_WITH_FAMILY].collect().filter(p => p.`fhir_id`.equals("22")).head
    val participantCentricWithNoFamily = participantCentrics.as[PATIENT_WITH_FAMILY].collect().filter(p => p.`fhir_id`.equals("33")).head

    participantCentricWith1Family.`families_id` shouldEqual Seq("FM_111")
    participantCentricWith2Families.`families_id` shouldEqual Seq("FM_111", "FM_222")
    participantCentricWithNoFamily.`families_id` shouldBe Seq.empty
  }

  "addDiagnosisPhenotypes" should "group phenotypes by observed or non-observed" in {

    val inputParticipants = Seq(
      PATIENT(participant_id = "A", fhir_id = "A"),
      PATIENT(participant_id = "B", fhir_id = "B")
    ).toDF()

    val inputPhenotypes = Seq(
      CONDITION_PHENOTYPE(fhir_id = "1p", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0001631")), observed = "positive"),
      CONDITION_PHENOTYPE(fhir_id = "2p", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING())),
      CONDITION_PHENOTYPE(fhir_id = "3p", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING()), observed = "not"),
      CONDITION_PHENOTYPE(fhir_id = "4p", participant_fhir_id = "A")
    ).toDF()

    val inputDiseases = Seq.empty[CONDITION_DISEASE].toDF()

    val output = inputParticipants.addDiagnosisPhenotypes(inputPhenotypes, inputDiseases)(allHpoTerms, allMondoTerms)
    val participantPhenotypes = output.select("participant_id", "phenotype").as[(String, Seq[PHENOTYPE])].collect()
    val participantA_Ph = participantPhenotypes.filter(_._1 == "A").head
    val participantB_Ph = participantPhenotypes.filter(_._1 == "B").head

    participantA_Ph._2.map(p => (p.fhir_id, p.observed)) should contain theSameElementsAs Seq(("1p", true), ("2p", false), ("3p", false))
    participantB_Ph._2.map(p => (p.fhir_id, p.observed)) should contain theSameElementsAs Nil
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
        observed = "positive"
      ),
      CONDITION_PHENOTYPE(
        fhir_id = "2p",
        participant_fhir_id = "A",
        condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0033127")),
        observed = "not",
        age_at_event = AGE_AT_EVENT(5)
      ),
      CONDITION_PHENOTYPE(
        fhir_id = "2p",
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

    val participantA_Ph = output.filter(_._1 == "A").head

    participantA_Ph._2.filter(t => t.`name` === "Phenotypic abnormality (HP:0000118)").head.`age_at_event_days` shouldEqual Seq(0)
    participantA_Ph._3.filter(t => t.`name` === "Phenotypic abnormality (HP:0000118)").head.`age_at_event_days` shouldEqual Seq(5, 10)
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
      PATIENT(`fhir_id` = "A", `participant_id` = "P_A"),
      PATIENT(`fhir_id` = "B", `participant_id` = "P_B")
    ).toDF()

    val output = inputBiospecimen.addBiospecimenParticipant(inputParticipant)

    val biospecimenWithParticipant = output.select("fhir_id", "participant").as[(String, PATIENT)].collect()
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
      PATIENT_WITH_FAMILY(`fhir_id` = "P1"),
      PATIENT_WITH_FAMILY(`fhir_id` = "P2"),
      PATIENT_WITH_FAMILY(`fhir_id` = "P3"),
      PATIENT_WITH_FAMILY(`fhir_id` = "P4"),
      PATIENT_WITH_FAMILY(`fhir_id` = "P5")
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

    val participantP1FileF1 = participant1._2.filter(_.`fhir_id` == "F1").head
    participantP1FileF1.`biospecimens`.map(_.`fhir_id`) == Seq("B11")
    participantP1FileF1.`biospecimens`.map(_.`fhir_id`) == Seq("B12")

    val participantP1FileF2 = participant1._2.filter(_.`fhir_id` == "F2").head
    participantP1FileF2.`biospecimens`.map(_.`fhir_id`) == Seq("B11")
    participantP1FileF2.`biospecimens`.map(_.`fhir_id`) == Seq("B13")

    val participantP2FileF1 = participant2._2.filter(_.`fhir_id` == "F1").head
    participantP2FileF1.`biospecimens`.map(_.`fhir_id`) == Seq("B21")

    val participantP2FileF3 = participant2._2.filter(_.`fhir_id` == "F3").head
    participantP2FileF3.`biospecimens`.map(_.`fhir_id`) == Seq("B22")

    val participantP2FileF5 = participant2._2.filter(_.`fhir_id` == "F5").head
    participantP2FileF5.`biospecimens`.isEmpty shouldBe true

    val participantP3FileF2 = participant3._2.filter(_.`fhir_id` == "F2").head
    participantP3FileF2.`biospecimens`.map(_.`fhir_id`) == Seq("B31")
    participantP3FileF2.`biospecimens`.map(_.`fhir_id`) == Seq("B32")

    val participantP3FileF4 = participant3._2.filter(_.`fhir_id` == "F4").head
    participantP3FileF4.`biospecimens`.map(_.`fhir_id`) == Seq("B33")

    val participantP3FileF5 = participant3._2.filter(_.`fhir_id` == "F5").head
    participantP3FileF5.`biospecimens`.isEmpty shouldBe true
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
      PATIENT_WITH_FAMILY(`fhir_id` = "P1"),
      PATIENT_WITH_FAMILY(`fhir_id` = "P2"),
      PATIENT_WITH_FAMILY(`fhir_id` = "P3"),
      PATIENT_WITH_FAMILY(`fhir_id` = "P4"),
      PATIENT_WITH_FAMILY(`fhir_id` = "P5")
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

    val output = inputDocumentReference.addFileParticipantsWithBiospecimen(inputParticipant, inputBiospecimen)

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
    fileF1ParticipantP1.`biospecimens`.map(_.`fhir_id`) == Seq("B11")
    fileF1ParticipantP1.`biospecimens`.map(_.`fhir_id`) == Seq("B12")

    val fileF1ParticipantP2 = file1._2.filter(_.`fhir_id` == "P2").head
    fileF1ParticipantP2.`biospecimens`.map(_.`fhir_id`) == Seq("B21")

    val fileF2ParticipantP1 = file2._2.filter(_.`fhir_id` == "P1").head
    fileF2ParticipantP1.`biospecimens`.map(_.`fhir_id`) == Seq("B11")
    fileF2ParticipantP1.`biospecimens`.map(_.`fhir_id`) == Seq("B13")

    val fileF2ParticipantP3 = file2._2.filter(_.`fhir_id` == "P3").head
    fileF2ParticipantP3.`biospecimens`.map(_.`fhir_id`) == Seq("B31")
    fileF2ParticipantP3.`biospecimens`.map(_.`fhir_id`) == Seq("B32")

    val fileF3ParticipantP2 = file3._2.filter(_.`fhir_id` == "P2").head
    fileF3ParticipantP2.`biospecimens`.map(_.`fhir_id`) == Seq("B22")

    val fileF4ParticipantP3 = file4._2.filter(_.`fhir_id` == "P3").head
    fileF4ParticipantP3.`biospecimens`.map(_.`fhir_id`) == Seq("B33")

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

    val output = inputBiospecimen.addBiospecimenFiles(inputDocumentReference)

    val biospecimenWithFiles = output.select("fhir_id", "files").as[(String, Seq[DOCUMENTREFERENCE])].collect()

    // Assertions
    // B1 -> F1 & F2
    // B2 -> F2
    // B3 -> no file
    // B_NOT_THERE is not in result

    val biospecimen1 = biospecimenWithFiles.filter(_._1 == "B1").head
    biospecimen1._2.map(_.`fhir_id`) == Seq("F1", "F2")

    val biospecimen2 = biospecimenWithFiles.filter(_._1 == "B2").head
    biospecimen2._2.map(_.`fhir_id`) == Seq("F2")

    val biospecimen3 = biospecimenWithFiles.filter(_._1 == "B3").head
    biospecimen3._2.isEmpty shouldBe true

    biospecimenWithFiles.exists(_._1 == "B_NOT_THERE") shouldBe false
  }

}
