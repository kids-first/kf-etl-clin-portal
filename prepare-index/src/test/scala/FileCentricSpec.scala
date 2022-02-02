import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader}
import bio.ferlab.fhir.etl.centricTypes.FileCentric
import model.{BIOSPECIMEN, DOCUMENTREFERENCE, FILE_CENTRIC, PARTICIPANT_WITH_BIOSPECIMEN, PATIENT, PATIENT_WITH_FAMILY, STUDY_CENTRIC}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

class FileCentricSpec extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/dev.conf")

  "transform" should "prepare index file_centric" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_documentreference_drs-document-reference" -> Seq(
        DOCUMENTREFERENCE(`fhir_id` = "11", `participant_fhir_ids` = Seq("1"), `specimen_fhir_ids` = Seq("111")),
        DOCUMENTREFERENCE(`fhir_id` = "12", `participant_fhir_ids` = Seq("1")),
        DOCUMENTREFERENCE(`fhir_id` = "21", `participant_fhir_ids` = Seq("2"), `specimen_fhir_ids` = Seq("222"))).toDF(),
      "normalized_specimen" -> Seq(
        BIOSPECIMEN(`fhir_id` = "111", `participant_fhir_id` = "1"),
        BIOSPECIMEN(`fhir_id` = "222", `participant_fhir_id` = "2")
      ).toDF(),
      "es_index_study_centric" -> Seq(STUDY_CENTRIC()).toDF(),
      "simple_participant" -> Seq(PATIENT_WITH_FAMILY(`fhir_id` = "1"), PATIENT_WITH_FAMILY(`fhir_id` = "2")).toDF(), // TODO Change me for SIMPLE_PARTICIPANT
    )

    val output = new FileCentric("re_000001", List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("es_index_file_centric")

    val file_centric = output("es_index_file_centric")
    file_centric.as[FILE_CENTRIC].collect() should contain theSameElementsAs
      Seq(
        FILE_CENTRIC(`fhir_id` = "11", `participant_fhir_ids` = Seq("1"),
          `participants` = Seq(PARTICIPANT_WITH_BIOSPECIMEN(`fhir_id` = "1",
            `biospecimens` = Seq(BIOSPECIMEN(`fhir_id` = "111", `participant_fhir_id` = "1"))))),
        FILE_CENTRIC(`fhir_id` = "12", `participant_fhir_ids` = Seq("1"),
          `participants` = Seq(PARTICIPANT_WITH_BIOSPECIMEN(`fhir_id` = "1",
            `biospecimens` = Seq.empty))),
        FILE_CENTRIC(`fhir_id` = "21", `participant_fhir_ids` = Seq("2"),
          `participants` = Seq(PARTICIPANT_WITH_BIOSPECIMEN(`fhir_id` = "2",
            `biospecimens` = Seq(BIOSPECIMEN(`fhir_id` = "222", `participant_fhir_id` = "2"))))))
  }

  "transform" should "ignore file linked to no participant" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_documentreference_drs-document-reference" -> Seq(
        DOCUMENTREFERENCE(`fhir_id` = "11", `participant_fhir_ids` = Seq("1"), `specimen_fhir_ids` = Seq("111")),
        DOCUMENTREFERENCE(`fhir_id` = "12", `participant_fhir_ids` = Seq("1")),
        DOCUMENTREFERENCE(`fhir_id` = "21", `participant_fhir_ids` = Seq("2"), `specimen_fhir_ids` = Seq("222"))).toDF(),
      "normalized_specimen" -> Seq(
        BIOSPECIMEN(`fhir_id` = "111", `participant_fhir_id` = "1"),
        BIOSPECIMEN(`fhir_id` = "222", `participant_fhir_id` = "2")
      ).toDF(),
      "es_index_study_centric" -> Seq(STUDY_CENTRIC()).toDF(),
      "simple_participant" -> Seq(PATIENT_WITH_FAMILY(`fhir_id` = "1")).toDF(), // TODO Change me for SIMPLE_PARTICIPANT
    )

    val output = new FileCentric("re_000001", List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("es_index_file_centric")

    val file_centric = output("es_index_file_centric")
    file_centric.as[FILE_CENTRIC].collect() should contain theSameElementsAs
      Seq(
        FILE_CENTRIC(`fhir_id` = "11", `participant_fhir_ids` = Seq("1"),
          `participants` = Seq(PARTICIPANT_WITH_BIOSPECIMEN(`fhir_id` = "1",
            `biospecimens` = Seq(BIOSPECIMEN(`fhir_id` = "111", `participant_fhir_id` = "1"))))),
        FILE_CENTRIC(`fhir_id` = "12", `participant_fhir_ids` = Seq("1"),
          `participants` = Seq(PARTICIPANT_WITH_BIOSPECIMEN(`fhir_id` = "1",
            `biospecimens` = Seq.empty))))
  }
}
