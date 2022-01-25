package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class StudyCentric(releaseId: String, studyIds: List[String])(implicit configuration: Configuration) extends ETL {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_study_centric")
  val normalized_researchstudy: DatasetConf = conf.getDataset("normalized_researchstudy")
  val normalized_documentreference: DatasetConf = conf.getDataset("normalized_documentreference")
  val normalized_patient: DatasetConf = conf.getDataset("normalized_patient")
  val normalized_group: DatasetConf = conf.getDataset("normalized_group")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      "normalized_researchstudy" ->
        read(s"${normalized_researchstudy.location}", "Parquet", Map(), None, None)
          .where(col("release_id") === releaseId)
          .where(col("study_id").isin(studyIds:_*)),
      "normalized_documentreference" ->
        read(s"${normalized_documentreference.location}", "Parquet", Map(), None, None)
          .where(col("release_id") === releaseId)
          .where(col("study_id").isin(studyIds:_*)),
      "normalized_patient" ->
        read(s"${normalized_patient.location}", "Parquet", Map(), None, None)
          .where(col("release_id") === releaseId)
          .where(col("study_id").isin(studyIds:_*)),
      "normalized_group" ->
        read(s"${normalized_group.location}", "Parquet", Map(), None, None)
          .where(col("release_id") === releaseId)
          .where(col("study_id").isin(studyIds:_*)),
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val studyDF = data("normalized_researchstudy")

    val countPatient = data("normalized_patient").count()
    val countFile = data("normalized_documentreference").count()
    val countFamily = data("normalized_group").count()

    val distinctOmics = Seq("TODO").toArray
    val distinctExperimentalStrategies = Seq("TODO").toArray
    val distinctDataAccess = Seq("TODO").toArray

    val transformedStudyDf = studyDF
      .withColumn("study_code", lit("TODO"))
      .withColumnRenamed("name", "study_name")
      .withColumn("program", lit("TODO"))
      .withColumn("type_of_omics", lit(distinctOmics))
      .withColumn("experimental_strategy", lit(distinctExperimentalStrategies))
      .withColumn("data_access", lit(distinctDataAccess))
      .withColumn("participant_count", lit(countPatient))
      .withColumn("file_count", lit(countFile))
      .withColumn("family_count", lit(countFamily))
      .withColumn("family_data", lit(countFamily > 0))

    transformedStudyDf.show(false)
    Map("es_index_study_centric" -> transformedStudyDf)
  }

  override def load(data: Map[String, DataFrame],
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    println(s"COUNT: ${data("es_index_study_centric").count()}")
    super.load(data)
  }
}
