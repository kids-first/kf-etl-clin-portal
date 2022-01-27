package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.fhir.etl.common.Utils._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class FileCentric(releaseId: String, studyIds: List[String])(implicit configuration: Configuration) extends ETL {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_file_centric")
  val normalized_documentreference: DatasetConf = conf.getDataset("normalized_documentreference")
  val simple_participant: DatasetConf = conf.getDataset("simple_participant")
  val es_index_study_centric: DatasetConf = conf.getDataset("es_index_study_centric")


  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      "normalized_documentreference" ->
        read(s"${normalized_documentreference.location}", "Parquet", Map(), None, None)
          .where(col("release_id") === releaseId)
          .where(col("study_id").isin(studyIds:_*)),
      "simple_participant" ->
        read(s"${simple_participant.location}", "Parquet", Map(), None, None)
          .where(col("release_id") === releaseId)
          .where(col("study_id").isin(studyIds:_*)),
      "es_index_study_centric" ->
        read(s"${es_index_study_centric.location}", "Parquet", Map(), None, None)
          .where(col("release_id") === releaseId)
          .where(col("study_id").isin(studyIds:_*)),
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val fileDF = data("normalized_documentreference")

    val transformedFile =
      fileDF
        .addStudy(data("es_index_study_centric"))
        .addParticipants(data("simple_participant")) // TODO add participants with their biospecimen (filter by file)
        .withColumn("type_of_omics", lit("TODO"))
        .withColumn("experimental_strategy", lit("TODO"))
        .withColumn("data_category", lit("TODO"))

    transformedFile.show(false)
    Map("es_index_file_centric" -> transformedFile)
  }

  override def load(data: Map[String, DataFrame],
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    println(s"COUNT: ${data("es_index_file_centric").count()}")
    val dataToLoad = Map("es_index_file_centric" -> data("es_index_file_centric")
      .sortWithinPartitions("fhir_id").toDF())
    super.load(dataToLoad)
  }
}
