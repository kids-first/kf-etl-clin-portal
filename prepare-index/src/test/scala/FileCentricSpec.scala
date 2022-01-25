import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader}
import bio.ferlab.fhir.etl.centricTypes.FileCentric
import model.{DOCUMENTREFERENCE, FILE_CENTRIC, PATIENT, STUDY_CENTRIC}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

class FileCentricSpec extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/dev.conf")

  "transform" should "prepare index file_centric" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_documentreference" -> Seq(
        DOCUMENTREFERENCE(`fhir_id` = "11", `participant_fhir_id` = "1"),
        DOCUMENTREFERENCE(`fhir_id` = "12", `participant_fhir_id` = "1"),
        DOCUMENTREFERENCE(`fhir_id` = "21", `participant_fhir_id` = "2")).toDF(),
      "es_index_study_centric" -> Seq(STUDY_CENTRIC()).toDF(),
      "simple_participant" -> Seq(PATIENT(`fhir_id` = "1"), PATIENT(`fhir_id` = "2")).toDF(), // TODO Change me for SIMPLE_PARTICIPANT
    )

    val output = new FileCentric("re_000001", List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("es_index_file_centric")

    val file_centric = output("es_index_file_centric")
    file_centric.as[FILE_CENTRIC].collect() should contain theSameElementsAs
      Seq(
        FILE_CENTRIC(`fhir_id` = "11", `participant_fhir_id` = "1"),
        FILE_CENTRIC(`fhir_id` = "12", `participant_fhir_id` = "1"),
        FILE_CENTRIC(`fhir_id` = "21", `participant_fhir_id` = "2"))
  }

  "transform" should "ignore file linked to no participant" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_documentreference" -> Seq(
        DOCUMENTREFERENCE(`fhir_id` = "11", `participant_fhir_id` = "1"),
        DOCUMENTREFERENCE(`fhir_id` = "12", `participant_fhir_id` = "1"),
        DOCUMENTREFERENCE(`fhir_id` = "21", `participant_fhir_id` = "2")).toDF(),
      "es_index_study_centric" -> Seq(STUDY_CENTRIC()).toDF(),
      "simple_participant" -> Seq(PATIENT(`fhir_id` = "1")).toDF(), // TODO Change me for SIMPLE_PARTICIPANT
    )

    val output = new FileCentric("re_000001", List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("es_index_file_centric")

    val file_centric = output("es_index_file_centric")
    file_centric.as[FILE_CENTRIC].collect() should contain theSameElementsAs
      Seq(
        FILE_CENTRIC(`fhir_id` = "11", `participant_fhir_id` = "1"),
        FILE_CENTRIC(`fhir_id` = "12", `participant_fhir_id` = "1"))
  }
}
