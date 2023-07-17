package bio.ferlab.etl.normalized.dataservice

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.etl.normalized.dataservice.model.{ESequencingCenter, ESequencingExperiment, ESequencingExperimentGenomicFile}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
/**
 * Use for initializing empty delta table, for project that does not use dataservice
 */
class EmptyDataserviceExportETL()(implicit override val conf: Configuration) extends ETL {
  val normalized_sequencing_experiment: DatasetConf = conf.getDataset("normalized_sequencing_experiment")
  override val mainDestination: DatasetConf = normalized_sequencing_experiment
  val normalized_sequencing_experiment_genomic_file: DatasetConf = conf.getDataset("normalized_sequencing_experiment_genomic_file")
  val normalized_sequencing_center: DatasetConf = conf.getDataset("normalized_sequencing_center")

  override def extract(lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    import spark.implicits._
    Map(
      normalized_sequencing_experiment.id -> spark.emptyDataset[ESequencingExperiment].toDF().withColumn("study_id", lit(null).cast("string")).withColumn("release_id", lit(null).cast("string")),
      normalized_sequencing_experiment_genomic_file.id -> spark.emptyDataset[ESequencingExperimentGenomicFile].toDF().withColumn("study_id", lit(null).cast("string")).withColumn("release_id", lit(null).cast("string")),
      normalized_sequencing_center.id -> spark.emptyDataset[ESequencingCenter].toDF().withColumn("study_id", lit(null).cast("string")).withColumn("release_id", lit(null).cast("string"))
    )
  }

  override def transform(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = data

}
