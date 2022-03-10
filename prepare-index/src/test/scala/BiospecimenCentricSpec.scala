import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader}
import bio.ferlab.fhir.etl.centricTypes.BiospecimenCentric
import model._
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

class BiospecimenCentricSpec extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/dev.conf")

  "transform" should "prepare index biospecimen_centric" in {
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
        BIOSPECIMEN(`fhir_id` = "222", `participant_fhir_id` = "2")
      ).toDF(),
      "es_index_study_centric" -> Seq(STUDY_CENTRIC()).toDF(),
      "normalized_task" -> Seq(TASK(`fhir_id` = "1", `document_reference_fhir_ids` = Seq("11", "21"))).toDF(),
    )

    val output = new BiospecimenCentric("re_000001", List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("es_index_biospecimen_centric")

    val biospecimen_centric = output("es_index_biospecimen_centric")
    biospecimen_centric.as[BIOSPECIMEN_CENTRIC].collect() should contain theSameElementsAs
      Seq(
        BIOSPECIMEN_CENTRIC(
          `fhir_id` = "111",
          `participant_fhir_id` = "1",
          `participant` = SIMPLE_PARTICIPANT(`fhir_id` = "1"),
          `files` = Seq(
            DOCUMENTREFERENCE_WITH_SEQ_EXP(
              `fhir_id` = "11",
              `participant_fhir_ids` = Seq("1"),
              `specimen_fhir_ids` = Seq("111"),
              `sequencing_experiment` = SEQUENCING_EXPERIMENT()
            )
          )
        ),
        BIOSPECIMEN_CENTRIC(
          `fhir_id` = "222",
          `participant_fhir_id` = "2",
          `participant` = SIMPLE_PARTICIPANT(`fhir_id` = "2"),
          `files` = Seq(
            DOCUMENTREFERENCE_WITH_SEQ_EXP(
              `fhir_id` = "21",
              `participant_fhir_ids` = Seq("2"),
              `specimen_fhir_ids` = Seq("222"),
              `sequencing_experiment` = SEQUENCING_EXPERIMENT()
            ))))
  }
}

