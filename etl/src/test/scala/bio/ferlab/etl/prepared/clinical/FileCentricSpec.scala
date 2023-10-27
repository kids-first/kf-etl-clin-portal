package bio.ferlab.etl.prepared.clinical

import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.etl.testmodels.enriched.ENRICHED_HISTOLOGY_DISEASE
import bio.ferlab.etl.testmodels.normalized._
import bio.ferlab.etl.testmodels.prepared._
import bio.ferlab.etl.testutils.WithTestSimpleConfiguration
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class FileCentricSpec extends AnyFlatSpec with Matchers with WithSparkSession with WithTestSimpleConfiguration {

  import spark.implicits._

  "transform" should "prepare index file_centric" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_document_reference" -> Seq(
        NORMALIZED_DOCUMENTREFERENCE(`fhir_id` = "11", `participant_fhir_id` = "1", `specimen_fhir_ids` = Seq("111")),
        NORMALIZED_DOCUMENTREFERENCE(`fhir_id` = "12", `participant_fhir_id` = "1"),
        NORMALIZED_DOCUMENTREFERENCE(`fhir_id` = "21", `participant_fhir_id` = "2", `specimen_fhir_ids` = Seq("222")),
        NORMALIZED_DOCUMENTREFERENCE(`fhir_id` = "33", `participant_fhir_id` = null, `specimen_fhir_ids` = Seq("111", "112", "222")),
        NORMALIZED_DOCUMENTREFERENCE(`fhir_id` = "44", `participant_fhir_id` = "1", `specimen_fhir_ids` = Seq("111", "112", "222"))
      ).toDF(),
      "normalized_specimen" -> Seq(
        NORMALIZED_BIOSPECIMEN(`fhir_id` = "111", `participant_fhir_id` = "1", container_id = Some("1")),
        NORMALIZED_BIOSPECIMEN(`fhir_id` = "111", `participant_fhir_id` = "1", container_id = Some("2")),
        NORMALIZED_BIOSPECIMEN(`fhir_id` = "112", `participant_fhir_id` = "1"),
        NORMALIZED_BIOSPECIMEN(`fhir_id` = "222", `participant_fhir_id` = "2")
      ).toDF(),
      "es_index_study_centric" -> Seq(PREPARED_STUDY()).toDF(),
      "simple_participant" -> Seq(
        PREPARED_SIMPLE_PARTICIPANT(
          `fhir_id` = "1",
          participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "1", participant_fhir_id_2 = "1")
        ),
        PREPARED_SIMPLE_PARTICIPANT(
          `fhir_id` = "2",
          participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "2", participant_fhir_id_2 = "2")
        )
      ).toDF(),
      "normalized_task" -> Seq(NORMALIZED_TASK(`fhir_id` = "1", `document_reference_fhir_ids` = Seq("11", "12")), NORMALIZED_TASK(`fhir_id` = "2", `document_reference_fhir_ids` = Seq("21"))).toDF(),
      "normalized_sequencing_experiment" -> Seq(NORMALIZED_SEQUENCING_EXPERIMENT()).toDF(),
      "normalized_sequencing_experiment_genomic_file" -> Seq(NORMALIZED_SEQUENCING_EXPERIMENT_GENOMIC_FILE()).toDF(),
      "enriched_histology_disease" -> Seq(ENRICHED_HISTOLOGY_DISEASE(`specimen_id` = "222")).toDF()
    )

    val output = FileCentric(defaultRuntime, List("SD_Z6MWD3H0")).transform(data)

    output.keys should contain("es_index_file_centric")

    val file_centric = output("es_index_file_centric").as[PREPARED_FILE].collect()

    file_centric.find(_.`fhir_id` == "11") shouldBe Some(
      PREPARED_FILE(`fhir_id` = "11",
        file_facet_ids = FILE_FACET_IDS(file_fhir_id_1 = "11", file_fhir_id_2 = "11"),
        `nb_participants` = 1,
        `nb_biospecimens` = 2,
        `participants` = Seq(PREPARED_FILE_PARTICIPANT(
          `fhir_id` = "1",
          participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "1", participant_fhir_id_2 = "1"),
          `biospecimens` = Set(
            PREPARED_BIOSPECIMEN_FOR_FILE(
              `fhir_id` = "111",
              biospecimen_facet_ids = BIOSPECIMEN_FACET_IDS(biospecimen_fhir_id_1 = "111", biospecimen_fhir_id_2 = "111"),
              `participant_fhir_id` = "1",
              container_id = Some("1")
            ),
            PREPARED_BIOSPECIMEN_FOR_FILE(
              `fhir_id` = "111",
              biospecimen_facet_ids = BIOSPECIMEN_FACET_IDS(biospecimen_fhir_id_1 = "111", biospecimen_fhir_id_2 = "111"),
              `participant_fhir_id` = "1",
              container_id = Some("2")
            )
          )
        ))
      )
    )
    file_centric.find(_.`fhir_id` == "12") shouldBe Some(
      PREPARED_FILE(`fhir_id` = "12",
        file_facet_ids = FILE_FACET_IDS(file_fhir_id_1 = "12", file_fhir_id_2 = "12"),
        `nb_participants` = 1,
        `nb_biospecimens` = 0,
        `participants` = Seq(PREPARED_FILE_PARTICIPANT(
          `fhir_id` = "1",
          participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "1", participant_fhir_id_2 = "1"),
          `biospecimens` = Set.empty[PREPARED_BIOSPECIMEN_FOR_FILE]))
      )
    )
    file_centric.find(_.`fhir_id` == "21") shouldBe Some(
      PREPARED_FILE(`fhir_id` = "21",
        file_facet_ids = FILE_FACET_IDS(file_fhir_id_1 = "21", file_fhir_id_2 = "21"),
        `nb_participants` = 1,
        `nb_biospecimens` = 1,
        `participants` = Seq(PREPARED_FILE_PARTICIPANT(`fhir_id` = "2",
          participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "2", participant_fhir_id_2 = "2"),
          `biospecimens` = Set(PREPARED_BIOSPECIMEN_FOR_FILE(
            `fhir_id` = "222",
            `biospecimen_facet_ids` = BIOSPECIMEN_FACET_IDS(biospecimen_fhir_id_1 = "222", biospecimen_fhir_id_2 = "222"),
            `participant_fhir_id` = "2",
            `diagnosis_mondo` = Some("MONDO:0005072"),
            `diagnosis_ncit` = Some("NCIT:0005072"),
            `diagnosis_icd` = Seq.empty,
            `source_text` = Some("Neuroblastoma"),
            `source_text_tumor_location` = Seq("Reported Unknown"),
          ))
        ))
      )
    )
    file_centric.find(_.`fhir_id` == "44") shouldBe Some(
      PREPARED_FILE(`fhir_id` = "44",
        file_facet_ids = FILE_FACET_IDS(file_fhir_id_1 = "44", file_fhir_id_2 = "44"),
        `nb_participants` = 2,
        `nb_biospecimens` = 4,
        `participants` = Seq(
          PREPARED_FILE_PARTICIPANT(`fhir_id` = "1",
            participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "1", participant_fhir_id_2 = "1"),
            `biospecimens` = Set(
              PREPARED_BIOSPECIMEN_FOR_FILE(
                `fhir_id` = "111",
                biospecimen_facet_ids = BIOSPECIMEN_FACET_IDS(biospecimen_fhir_id_1 = "111", biospecimen_fhir_id_2 = "111"),
                `participant_fhir_id` = "1",
                container_id = Some("1")
              ),
              PREPARED_BIOSPECIMEN_FOR_FILE(
                `fhir_id` = "111",
                biospecimen_facet_ids = BIOSPECIMEN_FACET_IDS(biospecimen_fhir_id_1 = "111", biospecimen_fhir_id_2 = "111"),
                `participant_fhir_id` = "1",
                container_id = Some("2")
              ),
              PREPARED_BIOSPECIMEN_FOR_FILE(
                `fhir_id` = "112",
                biospecimen_facet_ids = BIOSPECIMEN_FACET_IDS(biospecimen_fhir_id_1 = "112", biospecimen_fhir_id_2 = "112"),
                `participant_fhir_id` = "1"
              )


            )
          ),
          PREPARED_FILE_PARTICIPANT(
            `fhir_id` = "2",
            participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "2", participant_fhir_id_2 = "2"),
            `biospecimens` = Set(PREPARED_BIOSPECIMEN_FOR_FILE(
              `fhir_id` = "222",
              biospecimen_facet_ids = BIOSPECIMEN_FACET_IDS(biospecimen_fhir_id_1 = "222", biospecimen_fhir_id_2 = "222"),
              `participant_fhir_id` = "2",
              `diagnosis_mondo` = Some("MONDO:0005072"),
              `diagnosis_ncit` = Some("NCIT:0005072"),
              `diagnosis_icd` = Seq.empty,
              `source_text` = Some("Neuroblastoma"),
              `source_text_tumor_location` = Seq("Reported Unknown"),
            ))
          )
        )
      )
    )

    file_centric.find(_.`fhir_id` == "33") shouldBe Some(
      PREPARED_FILE(`fhir_id` = "33",
        file_facet_ids = FILE_FACET_IDS(file_fhir_id_1 = "33", file_fhir_id_2 = "33"),
        `nb_participants` = 2,
        `nb_biospecimens` = 4,
        `participants` = Seq(
          PREPARED_FILE_PARTICIPANT(`fhir_id` = "1",
            participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "1", participant_fhir_id_2 = "1"),
            `biospecimens` = Set(
              PREPARED_BIOSPECIMEN_FOR_FILE(
                `fhir_id` = "111",
                biospecimen_facet_ids = BIOSPECIMEN_FACET_IDS(biospecimen_fhir_id_1 = "111", biospecimen_fhir_id_2 = "111"),
                `participant_fhir_id` = "1",
                container_id = Some("1")
              ),
              PREPARED_BIOSPECIMEN_FOR_FILE(
                `fhir_id` = "111",
                biospecimen_facet_ids = BIOSPECIMEN_FACET_IDS(biospecimen_fhir_id_1 = "111", biospecimen_fhir_id_2 = "111"),
                `participant_fhir_id` = "1",
                container_id = Some("2")
              ),
              PREPARED_BIOSPECIMEN_FOR_FILE(
                `fhir_id` = "112",
                biospecimen_facet_ids = BIOSPECIMEN_FACET_IDS(biospecimen_fhir_id_1 = "112", biospecimen_fhir_id_2 = "112"),
                `participant_fhir_id` = "1"
              )
            )
          ),
          PREPARED_FILE_PARTICIPANT(
            `fhir_id` = "2",
            participant_facet_ids = PARTICIPANT_FACET_IDS(participant_fhir_id_1 = "2", participant_fhir_id_2 = "2"),
            `biospecimens` = Set(PREPARED_BIOSPECIMEN_FOR_FILE(
              `fhir_id` = "222",
              biospecimen_facet_ids = BIOSPECIMEN_FACET_IDS(biospecimen_fhir_id_1 = "222", biospecimen_fhir_id_2 = "222"),
              `participant_fhir_id` = "2",
              `diagnosis_mondo` = Some("MONDO:0005072"),
              `diagnosis_ncit` = Some("NCIT:0005072"),
              `diagnosis_icd` = Seq.empty,
              `source_text` = Some("Neuroblastoma"),
              `source_text_tumor_location` = Seq("Reported Unknown"),

            ))
          ))
      )
    )
  }

  "transform" should "ignore file linked to no participant" in {
    val data: Map[String, DataFrame] = Map(
      "normalized_document_reference" -> Seq(
        NORMALIZED_DOCUMENTREFERENCE(`fhir_id` = "11", `participant_fhir_id` = "1", `specimen_fhir_ids` = Seq("111")),
        NORMALIZED_DOCUMENTREFERENCE(`fhir_id` = "12", `participant_fhir_id` = "1"),
        NORMALIZED_DOCUMENTREFERENCE(`fhir_id` = "21", `participant_fhir_id` = "2", `specimen_fhir_ids` = Seq("222"))).toDF(),
      "normalized_specimen" -> Seq(
        NORMALIZED_BIOSPECIMEN(`fhir_id` = "111", `participant_fhir_id` = "1"),
        NORMALIZED_BIOSPECIMEN(`fhir_id` = "222", `participant_fhir_id` = "2")
      ).toDF(),
      "es_index_study_centric" -> Seq(PREPARED_STUDY()).toDF(),
      "simple_participant" -> Seq(PREPARED_SIMPLE_PARTICIPANT(`fhir_id` = "1")).toDF(),
      "normalized_task" -> Seq(NORMALIZED_TASK(`fhir_id` = "1")).toDF(),
      "normalized_sequencing_experiment" -> Seq(NORMALIZED_SEQUENCING_EXPERIMENT()).toDF(),
      "normalized_sequencing_experiment_genomic_file" -> Seq(NORMALIZED_SEQUENCING_EXPERIMENT_GENOMIC_FILE()).toDF(),
      "enriched_histology_disease" -> Seq(ENRICHED_HISTOLOGY_DISEASE(`specimen_id` = "x")).toDF()
    )

    val output = FileCentric(defaultRuntime, List("SD_Z6MWD3H0")).transform(data)

    output.keys should contain("es_index_file_centric")

    val file_centric = output("es_index_file_centric").as[PREPARED_FILE].collect()
    file_centric.find(_.`fhir_id` == "11") shouldBe Some(
      PREPARED_FILE(`fhir_id` = "11",
        file_facet_ids = FILE_FACET_IDS(file_fhir_id_1 = "11", file_fhir_id_2 = "11"),
        `nb_participants` = 1,
        `nb_biospecimens` = 1,
        `participants` = Seq(PREPARED_FILE_PARTICIPANT(`fhir_id` = "1",
          `biospecimens` = Set(PREPARED_BIOSPECIMEN_FOR_FILE(`fhir_id` = "111", `biospecimen_facet_ids` = BIOSPECIMEN_FACET_IDS(biospecimen_fhir_id_1 = "111", biospecimen_fhir_id_2 = "111"), `participant_fhir_id` = "1"))))))
    file_centric.find(_.`fhir_id` == "12") shouldBe Some(
      PREPARED_FILE(`fhir_id` = "12",
        file_facet_ids = FILE_FACET_IDS(file_fhir_id_1 = "12", file_fhir_id_2 = "12"),
        `nb_participants` = 1,
        `nb_biospecimens` = 0,
        `participants` = Seq(PREPARED_FILE_PARTICIPANT(`fhir_id` = "1",
          `biospecimens` = Set.empty[PREPARED_BIOSPECIMEN_FOR_FILE]))))
  }
}
