package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.fhir.etl.common.Utils._
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class FileCentric(studyIds: List[String])(implicit configuration: Configuration) extends ETLSingleDestination {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_file_centric")
  val normalized_drs_document_reference: DatasetConf = conf.getDataset("normalized_document_reference")
  val normalized_specimen: DatasetConf = conf.getDataset("normalized_specimen")
  val normalized_sequencing_experiment: DatasetConf = conf.getDataset("normalized_sequencing_experiment")
  val normalized_sequencing_experiment_genomic_file: DatasetConf = conf.getDataset("normalized_sequencing_experiment_genomic_file")
  val simple_participant: DatasetConf = conf.getDataset("simple_participant")
  val es_index_study_centric: DatasetConf = conf.getDataset("es_index_study_centric")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Seq(normalized_drs_document_reference, normalized_specimen, simple_participant, normalized_sequencing_experiment_genomic_file, normalized_sequencing_experiment,es_index_study_centric)
      .map(ds => ds.id -> ds.read
        .where(col("study_id").isin(studyIds: _*))
    ).toMap
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val fileDF = data(normalized_drs_document_reference.id)

    val transformedFile =
      fileDF
        .addStudy(data(es_index_study_centric.id))
        .addFileParticipantsWithBiospecimen(data(simple_participant.id), data(normalized_specimen.id))
        .withColumn("file_facet_ids", struct(col("fhir_id") as "file_fhir_id_1", col("fhir_id") as "file_fhir_id_2"))
        .addSequencingExperiment(data(normalized_sequencing_experiment.id), data(normalized_sequencing_experiment_genomic_file.id))

    transformedFile

  }
}
