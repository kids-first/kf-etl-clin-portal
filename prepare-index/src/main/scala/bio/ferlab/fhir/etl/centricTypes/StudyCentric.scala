package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import org.apache.spark.sql.functions.{array, coalesce, col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class StudyCentric(releaseId: String, studyIds: List[String])(implicit configuration: Configuration) extends ETL {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_study_centric")
  val normalized_researchstudy: DatasetConf = conf.getDataset("normalized_researchstudy")
  val normalized_drs_document_reference: DatasetConf = conf.getDataset("normalized_drs_document_reference")
  val normalized_patient: DatasetConf = conf.getDataset("normalized_patient")
  val normalized_group: DatasetConf = conf.getDataset("normalized_group")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Seq(normalized_researchstudy, normalized_drs_document_reference, normalized_patient, normalized_group)
      .map(ds => ds.id -> ds.read.where(col("release_id") === releaseId)
        .where(col("study_id").isin(studyIds: _*))
      ).toMap

  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val studyDF = data(normalized_researchstudy.id)

    val countPatientDf = data(normalized_patient.id).groupBy("study_id").count().withColumnRenamed("count", "participant_count")
    val countFileDf = data(normalized_drs_document_reference.id).groupBy("study_id").count().withColumnRenamed("count", "file_count")
    val countFamilyDf = data(normalized_group.id).groupBy("study_id").count().withColumnRenamed("count", "family_count")

    val distinctOmics = Seq("TODO").toArray
    val distinctExperimentalStrategies = Seq("TODO").toArray
    val distinctDataAccess = Seq("TODO").toArray

    val transformedStudyDf = studyDF
      .withColumnRenamed("name", "study_name")
      .withColumn("type_of_omics", lit(distinctOmics))
      .withColumn("experimental_strategy", lit(distinctExperimentalStrategies))
      .withColumn("data_access", lit(distinctDataAccess))
      .join(countPatientDf, Seq("study_id"), "left_outer")
      .withColumn("participant_count", coalesce(col("participant_count"), lit(0)))
      .join(countFileDf, Seq("study_id"), "left_outer")
      .withColumn("file_count", coalesce(col("file_count"), lit(0)))
      .join(countFamilyDf, Seq("study_id"), "left_outer")
      .withColumn("family_count", coalesce(col("family_count"), lit(0)))
      .withColumn("family_data", col("family_count").gt(0))

    transformedStudyDf.show(false)
    Map(mainDestination.id -> transformedStudyDf)
  }

  override def load(data: Map[String, DataFrame],
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    super.load(data)
  }
}
