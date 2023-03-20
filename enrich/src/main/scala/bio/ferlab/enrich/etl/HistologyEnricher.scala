package bio.ferlab.enrich.etl

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.functions.{col, collect_list, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class HistologyEnricher(studyIds: List[String])(implicit configuration: Configuration) extends ETLSingleDestination {
  override val mainDestination: DatasetConf = conf.getDataset("enriched_histology_disease")
  val normalized_disease: DatasetConf = conf.getDataset("normalized_disease")
  val normalized_histology_observation: DatasetConf = conf.getDataset("normalized_histology_observation")

  override def extract(lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    //FIXME duplicate accross project
    Seq(normalized_histology_observation, normalized_disease)
      .map(ds => ds.id -> ds.read
        .where(col("study_id").isin(studyIds: _*))
      ).toMap
  }

  override def transformSingle(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    val diseases = data(normalized_disease.id)
    val histologyObservation = data(normalized_histology_observation.id)
    histologyObservation
      .join(
        diseases,
        histologyObservation("condition_id") === diseases("fhir_id")
          and histologyObservation("study_id") === diseases("study_id")
      )
      .drop(diseases("fhir_id"))
      .drop(diseases("study_id"))
      .drop(diseases("release_id"))
      .drop(diseases("participant_fhir_id"))
  }
}