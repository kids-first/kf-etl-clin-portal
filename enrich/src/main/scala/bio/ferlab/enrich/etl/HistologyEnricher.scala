package bio.ferlab.enrich.etl

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.functions.{col, collect_list, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class HistologyEnricher(releaseId: String, studyIds: List[String])(implicit configuration: Configuration) extends ETLSingleDestination {
  override val mainDestination: DatasetConf = conf.getDataset("enriched_histology_disease")
  val normalized_disease_mondo: DatasetConf = conf.getDataset("normalized_disease_mondo")
  val normalized_disease_ncit: DatasetConf = conf.getDataset("normalized_disease_ncit")
  val normalized_histology_observation: DatasetConf = conf.getDataset("normalized_histology_observation")

  override def extract(lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    Seq(normalized_histology_observation, normalized_disease_mondo, normalized_disease_ncit)
      .map(ds => ds.id -> ds.read.where(col("release_id") === releaseId)
        .where(col("study_id").isin(studyIds: _*))
      ).toMap
  }

  override def transformSingle(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    val shapeDiseases = (diseasesDf: DataFrame) => diseasesDf
      .select(
        col("fhir_id"),
        struct(col("code")) as "disease")
    val shapedMondo = shapeDiseases(data(normalized_disease_mondo.id))
    val shapedNcit = shapeDiseases(data(normalized_disease_ncit.id))
    val histologyObservation = data(normalized_histology_observation.id)
    val shapeEnhancedSpecimen = (diseasesDf: DataFrame, diseasesColName: String) =>
      histologyObservation
        .join(diseasesDf, histologyObservation("condition_id") === diseasesDf("fhir_id"), "left")
        .drop(diseasesDf("fhir_id"))
        .groupBy(col("specimen_id"))
        .agg(collect_list(col("disease")) as diseasesColName)

    val specimenWithMondo = shapeEnhancedSpecimen(shapedMondo, "diseases_mondo")
    val specimenWithNcit = shapeEnhancedSpecimen(shapedNcit, "diseases_ncit")

    specimenWithMondo.join(specimenWithNcit, Seq("specimen_id"), "full")
  }


}