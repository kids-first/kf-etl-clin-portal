package bio.ferlab.etl.enriched.clinical

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import java.time.LocalDateTime

case class HistologyEnricher(rc: RuntimeETLContext, studyIds: List[String]) extends SimpleSingleETL(rc) {
  override val mainDestination: DatasetConf = conf.getDataset("enriched_histology_disease")
  val normalized_disease: DatasetConf = conf.getDataset("normalized_disease")
  val normalized_histology_observation: DatasetConf = conf.getDataset("normalized_histology_observation")

  override def extract(lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime): Map[String, DataFrame] = {
    //FIXME duplicate accross project
    Seq(normalized_histology_observation, normalized_disease)
      .map(ds => ds.id -> ds.read
        .where(col("study_id").isin(studyIds: _*))
      ).toMap
  }

  override def transformSingle(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime): DataFrame = {
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