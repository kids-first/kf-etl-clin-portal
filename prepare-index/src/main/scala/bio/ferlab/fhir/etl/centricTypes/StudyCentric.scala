package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class StudyCentric (batchId: String, loadType: String = "incremental")(implicit configuration: Configuration) extends ETL {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_study_centric")
  val normalized_researchstudy: DatasetConf = conf.getDataset("normalized_researchstudy")
  val normalized_documentreference: DatasetConf = conf.getDataset("normalized_documentreference")
  val normalized_patient: DatasetConf = conf.getDataset("normalized_patient")
  val normalized_group: DatasetConf = conf.getDataset("normalized_group")
  val storageOutput: StorageConf = conf.getStorage("output")


  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      "normalized_researchstudy" -> read(s"${storageOutput.path}${normalized_researchstudy.path}", "Parquet", Map(), None, None),
      "normalized_documentreference" -> read(s"${storageOutput.path}${normalized_documentreference.path}", "Parquet", Map(), None, None),
      "normalized_patient" -> read(s"${storageOutput.path}${normalized_patient.path}", "Parquet", Map(), None, None),
      "normalized_group" -> read(s"${storageOutput.path}${normalized_group.path}", "Parquet", Map(), None, None),
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val studyDF = data("normalized_researchstudy")

    val countPatient = data("normalized_patient").count()
    val countFile = data("normalized_documentreference").count()
    val countFamily = data("normalized_group").count()

    val transformedStudyDf = studyDF
      .withColumn("study_code", lit("TODO"))
      .withColumnRenamed("name", "study_name")
      .withColumn("program", lit("TODO"))
      .withColumn("type_of_omics", lit("TODO"))
      .withColumn("experimental_strategy", lit("TODO"))
      .withColumn("data_access", lit("TODO"))
      .withColumn("participant_count", lit(countPatient))
      .withColumn("file_count", lit(countFile))
      .withColumn("family_count", lit(countFamily))
      .withColumn("family_data", lit(countFamily > 0))
      .drop("status", "attribution", "version", "investigator_id")

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
