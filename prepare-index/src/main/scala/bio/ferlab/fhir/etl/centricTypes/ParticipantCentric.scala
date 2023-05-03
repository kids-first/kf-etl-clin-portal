package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.fhir.etl.common.Utils._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class ParticipantCentric(studyIds: List[String])(implicit configuration: Configuration) extends ETLSingleDestination {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_participant_centric")
  val simple_participant: DatasetConf = conf.getDataset("simple_participant")
  val normalized_drs_document_reference: DatasetConf = conf.getDataset("normalized_document_reference")
  val normalized_specimen: DatasetConf = conf.getDataset("normalized_specimen")
  val normalized_sequencing_experiment: DatasetConf = conf.getDataset("normalized_sequencing_experiment")
  val normalized_sequencing_experiment_genomic_file: DatasetConf = conf.getDataset("normalized_sequencing_experiment_genomic_file")
  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Seq(simple_participant, normalized_drs_document_reference, normalized_specimen, normalized_sequencing_experiment, normalized_sequencing_experiment_genomic_file)
      .map(ds => ds.id -> ds.read
                            .where(col("study_id").isin(studyIds: _*))
      ).toMap
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val patientDF = data(simple_participant.id)
    val filesWithSeqExp = data(normalized_drs_document_reference.id).addSequencingExperiment(data(normalized_sequencing_experiment.id), data(normalized_sequencing_experiment_genomic_file.id))

    val transformedParticipant =
      patientDF
        .withColumn("study_external_id", col("study")("external_id"))
        .addParticipantFilesWithBiospecimen(
          filesWithSeqExp,
          data(normalized_specimen.id)
        )

    transformedParticipant
  }

}
