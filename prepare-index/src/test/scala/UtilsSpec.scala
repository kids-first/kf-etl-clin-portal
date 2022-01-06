import bio.ferlab.fhir.etl.common.Utils._
import model.{BIOSPECIMEN, FAMILY, PARTICIPANT, PARTICIPANT_CENTRIC, STUDY}
import org.scalatest.{FlatSpec, Matchers}


class UtilsSpec extends FlatSpec with Matchers with WithSparkSession {
  import spark.implicits._

  case class ConditionCoding(code: String, category: String)

  val SCHEMA_CONDITION_CODING = "array<struct<category:string,code:string>>"

  "addStudy" should "add studies to participant" in {


    val inputStudies = Seq(STUDY()).toDF()
    val inputParticipants = Seq(PARTICIPANT()).toDF()

    val output = inputParticipants.addStudy(inputStudies)

    output.collect().sameElements(Seq(PARTICIPANT_CENTRIC()))
  }

  "addBiospecimen" should "add biospecimen to participant" in {

    val inputParticipants = Seq(
      PARTICIPANT(participant_id = "A", fhir_id = "A"),
      PARTICIPANT(participant_id = "B", fhir_id = "B")
    ).toDF()

    val inputBiospecimens = Seq(
      BIOSPECIMEN(fhir_id = "1", participant_fhir_id = "A"),
      BIOSPECIMEN(fhir_id = "2", participant_fhir_id = "A"),
      BIOSPECIMEN(fhir_id = "3", participant_fhir_id = "C")
    ).toDF()


    val output = inputParticipants.addBiospecimen(inputBiospecimens)
    val participantBio = output.select("participant_id","biospecimens.fhir_id").map(r => r.getString(0) -> r.getSeq[String](1)).collect()
    val participantA = participantBio.filter(_._1 == "A").head
    val participantB = participantBio.filter(_._1 == "B").head

    participantA._2 shouldEqual Seq("1", "2")
    participantB._2 shouldEqual Seq.empty[String]
  }

  "addFamily" should "add families to participant" in {

    val participantWith1Family = PARTICIPANT(`fhir_id` = "11")
    val participantWith2Families = PARTICIPANT(`fhir_id` = "22")
    val participantWithNoFamily = PARTICIPANT(`fhir_id` = "33")

    val family1 = FAMILY(`fhir_id` = "111", `family_id` = "FM_111", `family_members` = Seq(("11", false), ("22", false)), `family_members_id` = Seq("11", "22"))
    val family2 = FAMILY(`fhir_id` = "222", `family_id` = "FM_222", `family_members` = Seq(("22", false)), `family_members_id` = Seq("22"))

    val inputFamilies = Seq(family1, family2).toDF()
    val inputParticipants = Seq(participantWith1Family, participantWith2Families, participantWithNoFamily).toDF()

    val participantCentrics = inputParticipants.addFamily(inputFamilies)

    participantCentrics.count shouldEqual 3

    val participantCentricWith1Family = participantCentrics.filter(p => p.getString(5).equals("11")).head
    val participantCentricWith2Families = participantCentrics.filter(p => p.getString(5).equals("22")).head
    val participantCentricWithNoFamily = participantCentrics.filter(p => p.getString(5).equals("33")).head

    participantCentricWith1Family.getSeq(9) shouldEqual Seq("FM_111")
    participantCentricWith2Families.getSeq(9) shouldEqual Seq("FM_111", "FM_222")
    participantCentricWithNoFamily.getSeq(9) shouldBe Seq.empty
  }
}
