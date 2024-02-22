package bio.ferlab.etl.prepared.clinical

import bio.ferlab.etl.testmodels.normalized._
import bio.ferlab.etl.testmodels.prepared.{PREPARED_BIOSPECIMEN_FOR_FILE, PREPARED_STUDY}
import bio.ferlab.etl.testutils.WithTestSimpleConfiguration
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StudyCentricSpec extends AnyFlatSpec with Matchers with WithTestSimpleConfiguration {

  import spark.implicits._

  "transform" should "prepare index study_centric" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_research_study" -> Seq(NORMALIZED_RESEARCHSTUDY()).toDF(),
      "normalized_patient" -> Seq(NORMALIZED_PATIENT(), NORMALIZED_PATIENT()).toDF(),
      "normalized_document_reference" -> Seq(NORMALIZED_DOCUMENTREFERENCE(`experiment_strategy` = "RNASeq"), NORMALIZED_DOCUMENTREFERENCE(), NORMALIZED_DOCUMENTREFERENCE()).toDF(),
      "normalized_group" -> Seq(NORMALIZED_GROUP(), NORMALIZED_GROUP()).toDF(),
      "normalized_specimen" -> Seq(
        PREPARED_BIOSPECIMEN_FOR_FILE(fhir_id = "1", `specimen_id` = "id1"),
        PREPARED_BIOSPECIMEN_FOR_FILE(fhir_id = "2", `specimen_id` = "id2")
      ).toDF(),
      "normalized_sequencing_experiment" -> Seq(
        NORMALIZED_SEQUENCING_EXPERIMENT(experiment_strategy = "Whole Genome Sequencing"),
        NORMALIZED_SEQUENCING_EXPERIMENT(),
        NORMALIZED_SEQUENCING_EXPERIMENT(study_id = "unknown")
      ).toDF()
    )

    val output = StudyCentric(defaultRuntime, List("SD_Z6MWD3H0")).transform(data)

    output.keys should contain("es_index_study_centric")

    val study_centric = output("es_index_study_centric")
    study_centric.as[PREPARED_STUDY].collect() should contain theSameElementsAs
      Seq(
        PREPARED_STUDY(
          `participant_count` = 2,
          `file_count` = 3,
          `biospecimen_count` = 2,
          `experimental_strategy` = Seq("WGS", "RNASeq")
        )
      )
  }

  "transform" should "prepare inde study_centric with family_data false if no group" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_research_study" -> Seq(NORMALIZED_RESEARCHSTUDY()).toDF(),
      "normalized_patient" -> Seq(NORMALIZED_PATIENT(), NORMALIZED_PATIENT()).toDF(),
      "normalized_document_reference" -> Seq(NORMALIZED_DOCUMENTREFERENCE(), NORMALIZED_DOCUMENTREFERENCE(), NORMALIZED_DOCUMENTREFERENCE()).toDF(),
      "normalized_group" -> Seq[NORMALIZED_GROUP]().toDF(),
      "normalized_specimen" -> Seq(PREPARED_BIOSPECIMEN_FOR_FILE()).toDF(),
      "normalized_sequencing_experiment" -> Seq(NORMALIZED_SEQUENCING_EXPERIMENT()).toDF()
    )

    val output = StudyCentric(defaultRuntime, List("SD_Z6MWD3H0")).transform(data)

    output.keys should contain("es_index_study_centric")

    val study_centric = output("es_index_study_centric")
    study_centric.as[PREPARED_STUDY].collect() should contain theSameElementsAs
      Seq(PREPARED_STUDY(`participant_count` = 2, `file_count` = 3, `biospecimen_count` = 1))
  }

}
