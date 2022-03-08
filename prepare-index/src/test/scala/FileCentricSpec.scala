import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader}
import bio.ferlab.fhir.etl.centricTypes.FileCentric
import model._
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

class FileCentricSpec extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/dev.conf")

  "transform" should "prepare index file_centric" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_drs_document_reference" -> Seq(
        DOCUMENTREFERENCE(`fhir_id` = "11", `participant_fhir_ids` = Seq("1"), `specimen_fhir_ids` = Seq("111")),
        DOCUMENTREFERENCE(`fhir_id` = "12", `participant_fhir_ids` = Seq("1")),
        DOCUMENTREFERENCE(`fhir_id` = "21", `participant_fhir_ids` = Seq("2"), `specimen_fhir_ids` = Seq("222"))).toDF(),
      "normalized_specimen" -> Seq(
        BIOSPECIMEN(`fhir_id` = "111", `participant_fhir_id` = "1"),
        BIOSPECIMEN(`fhir_id` = "222", `participant_fhir_id` = "2")
      ).toDF(),
      "es_index_study_centric" -> Seq(STUDY_CENTRIC()).toDF(),
      "simple_participant" -> Seq(SIMPLE_PARTICIPANT(`fhir_id` = "1"), SIMPLE_PARTICIPANT(`fhir_id` = "2")).toDF(),
      "normalized_task" -> Seq(TASK(`fhir_id` = "1", `document_reference_fhir_ids` = Seq("11", "12") ), TASK(`fhir_id` = "2", `document_reference_fhir_ids` = Seq("21"))).toDF(),
    )

    val output = new FileCentric("re_000001", List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("es_index_file_centric")

    val file_centric = output("es_index_file_centric")

    file_centric.as[FILE_CENTRIC].collect() should contain theSameElementsAs
      Seq(
        FILE_CENTRIC(`fhir_id` = "11", `participant_fhir_ids` = Seq("1"),
          `participants` = Seq(PARTICIPANT_WITH_BIOSPECIMEN(`fhir_id` = "1",
            `biospecimens` = Seq(BIOSPECIMEN(
              `fhir_id` = "111",
              `participant_fhir_id` = "1"
            ))
          )),
          `sequencing_experiment` = SEQUENCING_EXPERIMENT(`fhir_id` = "1")
        ),
        FILE_CENTRIC(`fhir_id` = "12", `participant_fhir_ids` = Seq("1"),
          `participants` = Seq(PARTICIPANT_WITH_BIOSPECIMEN(`fhir_id` = "1",
            `biospecimens` = Seq.empty[BIOSPECIMEN])),
          `sequencing_experiment` = SEQUENCING_EXPERIMENT(`fhir_id` = "1")
        ),
        FILE_CENTRIC(`fhir_id` = "21", `participant_fhir_ids` = Seq("2"),
          `participants` = Seq(PARTICIPANT_WITH_BIOSPECIMEN(`fhir_id` = "2",
            `biospecimens` = Seq(BIOSPECIMEN(
              `fhir_id` = "222",
              `participant_fhir_id` = "2",
            ))
          )),
          `sequencing_experiment` = SEQUENCING_EXPERIMENT(`fhir_id` = "2")
        )
      )
  }

  "transform" should "ignore file linked to no participant" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_drs_document_reference" -> Seq(
        DOCUMENTREFERENCE(`fhir_id` = "11", `participant_fhir_ids` = Seq("1"), `specimen_fhir_ids` = Seq("111")),
        DOCUMENTREFERENCE(`fhir_id` = "12", `participant_fhir_ids` = Seq("1")),
        DOCUMENTREFERENCE(`fhir_id` = "21", `participant_fhir_ids` = Seq("2"), `specimen_fhir_ids` = Seq("222"))).toDF(),
      "normalized_specimen" -> Seq(
        BIOSPECIMEN(`fhir_id` = "111", `participant_fhir_id` = "1"),
        BIOSPECIMEN(`fhir_id` = "222", `participant_fhir_id` = "2")
      ).toDF(),
      "es_index_study_centric" -> Seq(STUDY_CENTRIC()).toDF(),
      "simple_participant" -> Seq(SIMPLE_PARTICIPANT(`fhir_id` = "1")).toDF(),
      "normalized_task" -> Seq(TASK(`fhir_id` = "1")).toDF(),
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
            `biospecimens` = Seq.empty[BIOSPECIMEN]))))
  }
}
