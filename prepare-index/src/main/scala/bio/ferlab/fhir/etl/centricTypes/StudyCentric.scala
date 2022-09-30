package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import org.apache.spark.sql.functions.{array, coalesce, col, collect_set, count, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class StudyCentric(releaseId: String, studyIds: List[String])(implicit configuration: Configuration) extends ETL {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_study_centric")
  val normalized_researchstudy: DatasetConf = conf.getDataset("normalized_research_study")
  val normalized_drs_document_reference: DatasetConf = conf.getDataset("normalized_document_reference")
  val normalized_patient: DatasetConf = conf.getDataset("normalized_patient")
  val normalized_group: DatasetConf = conf.getDataset("normalized_group")
  val normalized_specimen: DatasetConf = conf.getDataset("normalized_specimen")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Seq(normalized_researchstudy, normalized_drs_document_reference, normalized_patient, normalized_group, normalized_specimen)
      .map(ds => ds.id -> ds.read.where(col("release_id") === releaseId)
        .where(col("study_id").isin(studyIds: _*))
      ).toMap

  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val studyDF = data(normalized_researchstudy.id)

    val countPatientDf = data(normalized_patient.id).groupBy("study_id").count().withColumnRenamed("count", "participant_count")
    val countFamilyDf = data(normalized_group.id).groupBy("study_id").count()
      .withColumn("family_count", when(col("count").leq(1), lit(0)).otherwise(col("count")))
      .drop(col("count"))

    val countFileDf = data(normalized_drs_document_reference.id).groupBy("study_id")
      .agg( count(lit(1)) as "file_count",
        collect_set(col("experiment_strategy")) as "experimental_strategy",
        collect_set(col("data_category")) as "data_category",
        collect_set(col("controlled_access")) as "controlled_access"
    )

    val countBiospecimenDf = data(normalized_specimen.id)
      .groupBy("study_id")
      .agg(
        count(lit(1)) as "biospecimen_count",
      )

    val transformedStudyDf = studyDF
      .withColumnRenamed("name", "study_name")
      .join(countPatientDf, Seq("study_id"), "left_outer")
      .withColumn("participant_count", coalesce(col("participant_count"), lit(0)))
      .join(countFileDf, Seq("study_id"), "left_outer")
      .join(countBiospecimenDf, Seq("study_id"), "left_outer")
      .withColumn("file_count", coalesce(col("file_count"), lit(0)))
      .join(countFamilyDf, Seq("study_id"), "left_outer")
      .withColumn("family_count", coalesce(col("family_count"), lit(0)))
      .withColumn("family_data", col("family_count").gt(0))

    Map(mainDestination.id -> transformedStudyDf)
  }

  override def load(data: Map[String, DataFrame],
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    super.load(data)
  }
}
