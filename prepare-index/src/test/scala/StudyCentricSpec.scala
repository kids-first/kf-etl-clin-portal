import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader}
import bio.ferlab.fhir.etl.centricTypes.StudyCentric
import model._
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

class StudyCentricSpec extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/dev-include.conf")

  "transform" should "prepare index study_centric" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_research_study" -> Seq(RESEARCHSTUDY()).toDF(),
      "normalized_patient" -> Seq(PATIENT(), PATIENT()).toDF(),
      "normalized_document_reference" -> Seq(DOCUMENTREFERENCE(), DOCUMENTREFERENCE(), DOCUMENTREFERENCE()).toDF(),
      "normalized_group" -> Seq(GROUP(), GROUP()).toDF(),
      "normalized_specimen" -> Seq(
        BIOSPECIMEN(fhir_id = "1", `specimen_id` = "id1"),
        BIOSPECIMEN(fhir_id = "2", `specimen_id` = "id2")
      ).toDF()
    )

    val output = new StudyCentric("re_000001", List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("es_index_study_centric")

    val study_centric = output("es_index_study_centric")
    study_centric.as[STUDY_CENTRIC].collect() should contain theSameElementsAs
      Seq(STUDY_CENTRIC(`participant_count` = 2, `file_count` = 3, `family_count` = 0, `family_data` = false, `biospecimen_count` = 2))
  }

  "transform" should "prepare inde study_centric with family_data false if no group" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_research_study" -> Seq(RESEARCHSTUDY()).toDF(),
      "normalized_patient" -> Seq(PATIENT(), PATIENT()).toDF(),
      "normalized_document_reference" -> Seq(DOCUMENTREFERENCE(), DOCUMENTREFERENCE(), DOCUMENTREFERENCE()).toDF(),
      "normalized_group" -> Seq[GROUP]().toDF(),
      "normalized_specimen" -> Seq(BIOSPECIMEN()).toDF()
    )

    val output = new StudyCentric("re_000001", List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("es_index_study_centric")

    val study_centric = output("es_index_study_centric")
    study_centric.as[STUDY_CENTRIC].collect() should contain theSameElementsAs
      Seq(STUDY_CENTRIC(`participant_count` = 2, `file_count` = 3, `family_count` = 0, `family_data` = false, `biospecimen_count` = 1))
  }

}
