package bio.ferlab.etl.prepared.clinical

import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.etl.testmodels.enriched._
import bio.ferlab.etl.testmodels.normalized._
import bio.ferlab.etl.testmodels.prepared._
import bio.ferlab.etl.testutils.WithTestSimpleConfiguration
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BiospecimenCentricSpec extends AnyFlatSpec with Matchers with WithSparkSession with WithTestSimpleConfiguration {

  import spark.implicits._

  "transform" should "prepare index biospecimen_centric" in {
    val data: Map[String, DataFrame] = Map(
      "simple_participant" -> Seq(
        PREPARED_SIMPLE_PARTICIPANT(`fhir_id` = "1", participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "1", participant_fhir_id_2 = "1")),
        PREPARED_SIMPLE_PARTICIPANT(`fhir_id` = "2", participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "2", participant_fhir_id_2 = "2"))
      ).toDF(),
      "normalized_document_reference" -> Seq(
        NORMALIZED_DOCUMENTREFERENCE(`fhir_id` = "11", `participant_fhir_id` = "1", `specimen_fhir_ids` = Seq("111")),
        NORMALIZED_DOCUMENTREFERENCE(`fhir_id` = "12", `participant_fhir_id` = "1"),
        NORMALIZED_DOCUMENTREFERENCE(`fhir_id` = "21", `participant_fhir_id` = "2", `specimen_fhir_ids` = Seq("222")),
        NORMALIZED_DOCUMENTREFERENCE(`fhir_id` = "33", `participant_fhir_id` = null, `specimen_fhir_ids` = Seq("111", "222")),
        NORMALIZED_DOCUMENTREFERENCE(`fhir_id` = "44", `participant_fhir_id` = "1", `specimen_fhir_ids` = Seq("111", "222"))
      ).toDF(),

      "normalized_specimen" -> Seq(
        NORMALIZED_BIOSPECIMEN(`fhir_id` = "111", `participant_fhir_id` = "1"),
        NORMALIZED_BIOSPECIMEN(`fhir_id` = "222", `participant_fhir_id` = "2")
      ).toDF(),
      "es_index_study_centric" -> Seq(PREPARED_STUDY()).toDF(),
      "normalized_task" -> Seq(NORMALIZED_TASK(`fhir_id` = "1", `document_reference_fhir_ids` = Seq("11", "21"))).toDF(),
      "normalized_sequencing_experiment" -> Seq(NORMALIZED_SEQUENCING_EXPERIMENT()).toDF(),
      "normalized_sequencing_experiment_genomic_file" -> Seq(NORMALIZED_SEQUENCING_EXPERIMENT_GENOMIC_FILE()).toDF(),
      "enriched_histology_disease" -> Seq(ENRICHED_HISTOLOGY_DISEASE(`specimen_id` = "111")).toDF()
    )

    val output = BiospecimenCentric(defaultRuntime, List("SD_Z6MWD3H0")).transform(data)

    output.keys should contain("es_index_biospecimen_centric")

    val biospecimen_centric = output("es_index_biospecimen_centric")

    val result = biospecimen_centric.as[PREPARED_BIOSPECIMEN].collect()
    result.length shouldBe 2
    result.find(b => b.fhir_id == "111") shouldBe Some(
      PREPARED_BIOSPECIMEN(
        `fhir_id` = "111",
        `diagnosis_mondo` = "MONDO:0005072",
        `diagnosis_ncit` = "NCIT:0005072",
        `diagnosis_icd` = Nil,
        `source_text` = "Neuroblastoma",
        `source_text_tumor_location` = Seq("Reported Unknown"),
        biospecimen_facet_ids = BIOSPECIMEN_FACET_IDS(biospecimen_fhir_id_1 = "111", biospecimen_fhir_id_2 = "111"),
        `participant_fhir_id` = "1",
        `participant` = PREPARED_SIMPLE_PARTICIPANT(`fhir_id` = "1", participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "1", participant_fhir_id_2 = "1")),
        `nb_files` = 3,
        `files` = Seq(
          PREPARED_FILES_FOR_BIOSPECIMEN(
            `fhir_id` = "11",
            file_facet_ids = FILE_FACET_IDS(file_fhir_id_1 = "11", file_fhir_id_2 = "11")
          ),
          PREPARED_FILES_FOR_BIOSPECIMEN(
            `fhir_id` = "33",
            file_facet_ids = FILE_FACET_IDS(file_fhir_id_1 = "33", file_fhir_id_2 = "33")
          ),
          PREPARED_FILES_FOR_BIOSPECIMEN(
            `fhir_id` = "44",
            file_facet_ids = FILE_FACET_IDS(file_fhir_id_1 = "44", file_fhir_id_2 = "44")
          )
        )
      )
    )
    result.find(b => b.fhir_id == "222") shouldBe Some(
      PREPARED_BIOSPECIMEN(
        `fhir_id` = "222",
        biospecimen_facet_ids = BIOSPECIMEN_FACET_IDS(biospecimen_fhir_id_1 = "222", biospecimen_fhir_id_2 = "222"),
        `participant_fhir_id` = "2",
        `participant` = PREPARED_SIMPLE_PARTICIPANT(`fhir_id` = "2", participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "2", participant_fhir_id_2 = "2")),
        `nb_files` = 3,
        `files` = Seq(
          PREPARED_FILES_FOR_BIOSPECIMEN(
            `fhir_id` = "21",
            file_facet_ids = FILE_FACET_IDS(file_fhir_id_1 = "21", file_fhir_id_2 = "21")
          ),
          PREPARED_FILES_FOR_BIOSPECIMEN(
            `fhir_id` = "33",
            file_facet_ids = FILE_FACET_IDS(file_fhir_id_1 = "33", file_fhir_id_2 = "33")
          ),
          PREPARED_FILES_FOR_BIOSPECIMEN(
            `fhir_id` = "44",
            file_facet_ids = FILE_FACET_IDS(file_fhir_id_1 = "44", file_fhir_id_2 = "44")
          )
        )
      ))
  }
}

