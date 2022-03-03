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
      "normalized_drs_document_reference" -> Seq(
        DOCUMENTREFERENCE(`fhir_id` = "11", `participant_fhir_ids` = Seq("1"), `specimen_fhir_ids` = Seq("111")),
        DOCUMENTREFERENCE(`fhir_id` = "12", `participant_fhir_ids` = Seq("1")),
        DOCUMENTREFERENCE(`fhir_id` = "21", `participant_fhir_ids` = Seq("2"), `specimen_fhir_ids` = Seq("222"))).toDF(),
      "normalized_specimen" -> Seq(
        BIOSPECIMEN(`fhir_id` = "111", `participant_fhir_id` = "1"),
        BIOSPECIMEN(`fhir_id` = "222", `participant_fhir_id` = "2")
      ).toDF(),
      "normalized_task" -> Seq(
        TASK(biospecimen_fhir_ids = Seq("111"), document_reference_fhir_ids = Seq("1")),
      ).toDF(),
      "es_index_study_centric" -> Seq(STUDY_CENTRIC()).toDF(),
    )

    val output = new ParticipantCentric("re_000001", List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("es_index_participant_centric")

    val participant_centric = output("es_index_participant_centric")

    participant_centric.as[PARTICIPANT_CENTRIC].collect() should contain theSameElementsAs
      Seq(
        PARTICIPANT_CENTRIC(
          `fhir_id` = "1",
          `files` = Seq(
            FILE_WITH_BIOSPECIMEN(
              `fhir_id` = "11",
              `participant_fhir_ids` = Seq("1"),
              `specimen_fhir_ids` = Seq("111"),
              `biospecimens` = Seq(
                BIOSPECIMEN_WITH_SEQ_EXPERIMENTS(
                  `fhir_id` = "111",
                  `participant_fhir_id` = "1",
                  sequencing_experiments = Seq(
                    SEQUENCING_EXPERIMENT()
                )
                ))),
            FILE_WITH_BIOSPECIMEN(
              `fhir_id` = "12",
              `participant_fhir_ids` = Seq("1"),
              `specimen_fhir_ids` = Seq.empty,
              `biospecimens` = Seq.empty
            ))
        ),
        PARTICIPANT_CENTRIC(
          `fhir_id` = "2",
          `files` = Seq(
            FILE_WITH_BIOSPECIMEN(`fhir_id` = "21",
              `participant_fhir_ids` = Seq("2"),
              `specimen_fhir_ids` = Seq("222"),
              `biospecimens` = Seq(
                BIOSPECIMEN_WITH_SEQ_EXPERIMENTS(`fhir_id` = "222", `participant_fhir_id` = "2"))))))
  }
}
