import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader}
import bio.ferlab.fhir.etl.centricTypes.ParticipantCentric
import model._
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

class ParticipantCentricSpec extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/dev.conf")

  "transform" should "prepare index participant_centric" in {
    val data: Map[String, DataFrame] = Map(
      "simple_participant" -> Seq(
        SIMPLE_PARTICIPANT(`fhir_id` = "1"),
        SIMPLE_PARTICIPANT(`fhir_id` = "2")
      ).toDF(),
      "normalized_document_reference" -> Seq(
        DOCUMENTREFERENCE(`fhir_id` = "11", `participant_fhir_ids` = Seq("1"), `specimen_fhir_ids` = Seq("111")),
        DOCUMENTREFERENCE(`fhir_id` = "12", `participant_fhir_ids` = Seq("1")),
        DOCUMENTREFERENCE(`fhir_id` = "21", `participant_fhir_ids` = Seq("2"), `specimen_fhir_ids` = Seq("222"))).toDF(),
      "normalized_specimen" -> Seq(
        BIOSPECIMEN(`fhir_id` = "111", `participant_fhir_id` = "1"),
        BIOSPECIMEN(`fhir_id` = "222", `participant_fhir_id` = "2"),
        BIOSPECIMEN(`fhir_id` = "333", `participant_fhir_id` = "1")
      ).toDF(),
      "normalized_task" -> Seq(
        TASK(fhir_id = "1", biospecimen_fhir_ids = Seq("111"), document_reference_fhir_ids = Seq("11", "12")),
        TASK(fhir_id = "2", biospecimen_fhir_ids = Seq("222"), document_reference_fhir_ids = Seq("21"))
      ).toDF(),
      "es_index_study_centric" -> Seq(STUDY_CENTRIC()).toDF(),
    )

    val output = new ParticipantCentric("re_000001", List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("es_index_participant_centric")

    val participant_centric = output("es_index_participant_centric").as[PARTICIPANT_CENTRIC].collect()

    participant_centric.find(_.`fhir_id` == "1").map(p=> p.copy(files =  p.files.sortBy(_.`fhir_id`).reverse)) shouldBe Some(
      PARTICIPANT_CENTRIC(
        `fhir_id` = "1",
        `nb_files` = 2,
        `nb_biospecimens` = 2,
        `files` = Seq(
          FILE_WITH_BIOSPECIMEN(
            `fhir_id` = Some("12"),
            `participant_fhir_ids` = Seq("1"),
            `specimen_fhir_ids` = Some(Nil),
            `biospecimens` = Seq.empty,
            `sequencing_experiment` = Some(SEQUENCING_EXPERIMENT())
          ),
          FILE_WITH_BIOSPECIMEN(
            `fhir_id` = Some("11"),
            `participant_fhir_ids` = Seq("1"),
            `specimen_fhir_ids` = Some(Seq("111")),
            `biospecimens` = Seq(
              BIOSPECIMEN(
                `fhir_id` = "111",
                `participant_fhir_id` = "1",
              )),
            `sequencing_experiment` = Some(SEQUENCING_EXPERIMENT())
          ),
          FILE_WITH_BIOSPECIMEN(
            `fhir_id` = None,
            `acl` = None,
            `access_urls` = None,
            `controlled_access` = None,
            `data_type` = None,
            `external_id` = None,
            `file_format` = None,
            `file_id` = None,
            `hashes` = None,
            `is_harmonized` = None,
            `latest_did` = None,
            `repository` = None,
            `size` = None,
            `urls` = None,
            `study_id` = None,
            `file_name` = Some("dummy_file"),
            `participant_fhir_ids` = null,
            `specimen_fhir_ids` = None,
            `release_id` = None,
            `biospecimens` = Seq(
              BIOSPECIMEN(
                `fhir_id` = "333",
                `participant_fhir_id` = "1",
              )),
            `sequencing_experiment` = None
          ))
      )
    )
    participant_centric.find(_.`fhir_id` == "2") shouldBe Some(
      PARTICIPANT_CENTRIC(
        `fhir_id` = "2",
        `nb_files` = 1,
        `nb_biospecimens` = 1,
        `files` = Seq(
          FILE_WITH_BIOSPECIMEN(
            `fhir_id` = Some("21"),
            `participant_fhir_ids` = Seq("2"),
            `specimen_fhir_ids` = Some(Seq("222")),
            `biospecimens` = Seq(
              BIOSPECIMEN(`fhir_id` = "222", `participant_fhir_id` = "2")),
            `sequencing_experiment` = Some(SEQUENCING_EXPERIMENT())
          )))
    )
  }
}
