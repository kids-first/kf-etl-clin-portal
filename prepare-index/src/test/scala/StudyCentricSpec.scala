import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader}
import bio.ferlab.fhir.etl.centricTypes.StudyCentric
import model._
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

class StudyCentricSpec extends FlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/dev.conf")

  "transform" should "prepare index study_centric" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_researchstudy" -> Seq(RESEARCHSTUDY()).toDF(),
      "normalized_patient" -> Seq(PATIENT(), PATIENT()).toDF(),
      "normalized_documentreference" -> Seq(DOCUMENTREFERENCE(), DOCUMENTREFERENCE(), DOCUMENTREFERENCE()).toDF(),
      "normalized_group" -> Seq(FAMILY(), FAMILY()).toDF()
    )

    val output = new StudyCentric("re_000001")(conf).transform(data)

    output.keys should contain("es_index_study_centric")

    val study_centric = output("es_index_study_centric")
    study_centric.as[STUDY_CENTRIC].collect() should contain theSameElementsAs
      Seq(STUDY_CENTRIC(`participant_count` = 2, `file_count` = 3, `family_count` = 2, `family_data` = true))
  }
  
  "transform" should "prepare index study_centric with family_data false if no group" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_researchstudy" -> Seq(RESEARCHSTUDY()).toDF(),
      "normalized_patient" -> Seq(PATIENT(), PATIENT()).toDF(),
      "normalized_documentreference" -> Seq(DOCUMENTREFERENCE(), DOCUMENTREFERENCE(), DOCUMENTREFERENCE()).toDF(),
      "normalized_group" -> Seq[FAMILY]().toDF()
    )

    val output = new StudyCentric("re_000001")(conf).transform(data)

    output.keys should contain("es_index_study_centric")

    val study_centric = output("es_index_study_centric")
    study_centric.as[STUDY_CENTRIC].collect() should contain theSameElementsAs
      Seq(STUDY_CENTRIC(`participant_count` = 2, `file_count` = 3, `family_count` = 0, `family_data` = false))
  }

}
