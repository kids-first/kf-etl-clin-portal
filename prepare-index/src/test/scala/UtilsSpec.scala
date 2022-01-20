import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.fhir.etl.common.Utils._
import model._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode, explode_outer}
import org.scalatest.{FlatSpec, Matchers}

class UtilsSpec extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  case class ConditionCoding(code: String, category: String)

  val SCHEMA_CONDITION_CODING = "array<struct<category:string,code:string>>"
  val allHpoTerms: DataFrame = read("./prepare-index/src/test/resources/hpo_terms.json", "Json", Map(), None, None)
  val allMondoTerms: DataFrame = read("./prepare-index/src/test/resources/mondo_terms.json", "Json", Map(), None, None)

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
        .select("participant_id", "diagnoses")
        .withColumn("diagnoses_exp", explode_outer(col("diagnoses")))
        .select("participant_id", "diagnoses_exp.diagnosis_id")
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

  it should "group diagnoses by age at event days" in {
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

    participantA_Ph._2.filter(t => t.`name` === "disease or disorder (MONDO:0000001)").head.`age_at_event_days` shouldEqual Seq(5,10)
  }

  "addParticipant" should "add participant to file" in {
    val inputDocumentReference = Seq(
      DOCUMENTREFERENCE(`participant_fhir_id` = "A", `fhir_id` = "1"),
      DOCUMENTREFERENCE(`participant_fhir_id` = "B", `fhir_id` = "2"),
      DOCUMENTREFERENCE(`participant_fhir_id` = "C", `fhir_id` = "3")
    ).toDF()

    val inputParticipant = Seq(
      PATIENT(`fhir_id` = "A", `participant_id` = "P_A"),
      PATIENT(`fhir_id` = "B", `participant_id` = "P_B")
    ).toDF()

    val output = inputDocumentReference.addParticipant(inputParticipant)

    val fileWithParticipant = output.select("fhir_id", "participant").as[(String, Seq[PATIENT])].collect()
    val file1 = fileWithParticipant.filter(_._1 == "1").head
    val file2 = fileWithParticipant.filter(_._1 == "2").head

    file1._2.map(_.`participant_id`) shouldEqual Seq("P_A")
    file2._2.map(_.`participant_id`) shouldEqual Seq("P_B")

    // Ignore file without participant
    fileWithParticipant.exists(_._1 == "3") shouldEqual false
  }
}
