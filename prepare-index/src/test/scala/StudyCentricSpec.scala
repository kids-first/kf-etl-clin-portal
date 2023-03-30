import bio.ferlab.fhir.etl.centricTypes.StudyCentric
import model._
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StudyCentricSpec extends AnyFlatSpec with Matchers with WithSparkSession  with WithTestConfig {

  import spark.implicits._

  "transform" should "prepare index study_centric" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_research_study" -> Seq(RESEARCHSTUDY()).toDF(),
      "normalized_patient" -> Seq(PATIENT(), PATIENT()).toDF(),
      "normalized_document_reference" -> Seq(DOCUMENTREFERENCE(), DOCUMENTREFERENCE(), DOCUMENTREFERENCE()).toDF(),
      "normalized_group" -> Seq(GROUP(), GROUP()).toDF(),
      "normalized_specimen" -> Seq(
        BIOSPECIMEN(fhir_id = "1", `specimen_id` = "id1"),
        BIOSPECIMEN(fhir_id = "2", `specimen_id` = "id2")
      ).toDF(),
      "normalized_sequencing_experiment" -> Seq(
        SEQUENCING_EXPERIMENT_INPUT(experiment_strategy = "Whole Genome Sequencing"),
        SEQUENCING_EXPERIMENT_INPUT(),
        SEQUENCING_EXPERIMENT_INPUT(study_id = "unknown")
      ).toDF()
    )

    val output = new StudyCentric(List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("es_index_study_centric")

    val study_centric = output("es_index_study_centric")
    study_centric.as[STUDY_CENTRIC].collect() should contain theSameElementsAs
      Seq(
        STUDY_CENTRIC(
          `participant_count` = 2,
          `file_count` = 3,
          `biospecimen_count` = 2,
          `experimental_strategy` = Seq("WGS", "Whole Genome Sequencing")
        )
      )
  }

  "transform" should "prepare inde study_centric with family_data false if no group" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_research_study" -> Seq(RESEARCHSTUDY()).toDF(),
      "normalized_patient" -> Seq(PATIENT(), PATIENT()).toDF(),
      "normalized_document_reference" -> Seq(DOCUMENTREFERENCE(), DOCUMENTREFERENCE(), DOCUMENTREFERENCE()).toDF(),
      "normalized_group" -> Seq[GROUP]().toDF(),
      "normalized_specimen" -> Seq(BIOSPECIMEN()).toDF(),
      "normalized_sequencing_experiment" -> Seq(SEQUENCING_EXPERIMENT_INPUT()).toDF()
    )

    val output = new StudyCentric(List("SD_Z6MWD3H0"))(conf).transform(data)

    output.keys should contain("es_index_study_centric")

    val study_centric = output("es_index_study_centric")
    study_centric.as[STUDY_CENTRIC].collect() should contain theSameElementsAs
      Seq(STUDY_CENTRIC(`participant_count` = 2, `file_count` = 3, `biospecimen_count` = 1))
  }

}
