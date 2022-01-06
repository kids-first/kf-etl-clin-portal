import bio.ferlab.datalake.spark3.loader.GenericLoader.{insert, read}
import bio.ferlab.fhir.etl.common.Utils._
import model.{BIOSPECIMEN, CONDITION, CONDITION_CODING, PARTICIPANT, PARTICIPANT_CENTRIC, PHENOTYPE, STUDY}
import org.apache.spark.sql.Row
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

  "addDiagnosisPhenotypes" should "group phenotypes by observed or non-observed" in {
    val allHpoTerms = read("./prepare-index/src/test/resources/hpo_terms.json", "Json", Map(), None, None)
    val allMondoTerms = read("./prepare-index/src/test/resources/mondo_terms.json", "Json", Map(), None, None)

    val inputParticipants = Seq(
      PARTICIPANT(participant_id = "A", fhir_id = "A"),
      PARTICIPANT(participant_id = "B", fhir_id = "B")
    ).toDF()

    val inputConditions = Seq(
      CONDITION(fhir_id = "1", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING(`category` = "HPO", `code` = "HP_0001631")) ,observed = "positive", condition_profile = "phenotype"),
      CONDITION(fhir_id = "2", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING()), condition_profile = "phenotype"),
      CONDITION(fhir_id = "3", participant_fhir_id = "A", condition_coding = Seq(CONDITION_CODING()),observed = "not", condition_profile = "phenotype"),
      CONDITION(fhir_id = "4", participant_fhir_id = "A", condition_profile = "phenotype")
    ).toDF()

    val output = inputParticipants.addDiagnosisPhenotypes(inputConditions)(allHpoTerms, allMondoTerms)

    val participantPhenotypes = output.select("participant_id","phenotype").as[(String, Seq[PHENOTYPE])].collect()
    val participantA = participantPhenotypes.filter(_._1 == "A").head
    val participantB = participantPhenotypes.filter(_._1 == "B").head

    participantA._2.map(p => (p.fhir_id, p.observed)) shouldEqual Seq(("1", true), ("2", false), ("3", false))
    participantB._2.map(p => (p.fhir_id, p.observed)) shouldEqual Nil
  }
}
