import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.fhir.etl.common.Utils._
import model._
import org.scalatest.{FlatSpec, Matchers}

class UtilsSpec extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  case class ConditionCoding(code: String, category: String)

  val SCHEMA_CONDITION_CODING = "array<struct<category:string,code:string>>"

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
    val allHpoTerms = read("./prepare-index/src/test/resources/hpo_terms.json", "Json", Map(), None, None)
    val allMondoTerms = read("./prepare-index/src/test/resources/mondo_terms.json", "Json", Map(), None, None)

    val inputParticipants = Seq(
      PATIENT(participant_id = "A", fhir_id = "A"),
      PATIENT(participant_id = "B", fhir_id = "B")
    ).toDF()

    val inputConditions = Seq(
      CONDITION(fhir_id = "1", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0001631")), observed = "positive", condition_profile = "phenotype"),
      CONDITION(fhir_id = "2", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING()), condition_profile = "phenotype"),
      CONDITION(fhir_id = "3", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING()), observed = "not", condition_profile = "phenotype"),
      CONDITION(fhir_id = "4", participant_fhir_id = "A", condition_profile = "phenotype")
    ).toDF()

    val output = inputParticipants.addDiagnosisPhenotypes(inputConditions)(allHpoTerms, allMondoTerms)

    val participantPhenotypes = output.select("participant_id", "phenotype").as[(String, Seq[PHENOTYPE])].collect()
    val participantA = participantPhenotypes.filter(_._1 == "A").head
    val participantB = participantPhenotypes.filter(_._1 == "B").head

    participantA._2.map(p => (p.fhir_id, p.observed)) shouldEqual Seq(("1", true), ("2", false), ("3", false))
    participantB._2.map(p => (p.fhir_id, p.observed)) shouldEqual Nil
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
