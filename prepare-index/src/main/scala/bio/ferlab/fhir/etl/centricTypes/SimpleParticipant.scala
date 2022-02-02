package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.fhir.etl.common.Utils._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class SimpleParticipant(releaseId: String, studyIds: List[String])(implicit configuration: Configuration) extends ETL {

  override val mainDestination: DatasetConf = conf.getDataset("simple_participant")
  val normalized_patient: DatasetConf = conf.getDataset("normalized_patient")
  val normalized_observation_vital_status: DatasetConf = conf.getDataset("normalized_observation_vital-status")
  val normalized_condition_phenotype: DatasetConf = conf.getDataset("normalized_condition_phenotype")
  val normalized_condition_disease: DatasetConf = conf.getDataset("normalized_condition_disease")
  val normalized_group: DatasetConf = conf.getDataset("normalized_group")
  val hpoTermsConf: DatasetConf = conf.getDataset("hpo_terms")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      normalized_patient.id ->
        read(s"${normalized_patient.location}", "Parquet", Map(), None, None)
          .where(col("release_id") === releaseId)
          .where(col("study_id").isin(studyIds:_*)),
      normalized_observation_vital_status.id ->
        read(s"${normalized_observation_vital_status.location}", "Parquet", Map(), None, None)
          .where(col("release_id") === releaseId)
          .where(col("study_id").isin(studyIds:_*)),
      normalized_condition_phenotype.id ->
        read(s"${normalized_condition_phenotype.location}", "Parquet", Map(), None, None)
          .where(col("release_id") === releaseId)
          .where(col("study_id").isin(studyIds:_*)),
      normalized_condition_disease.id ->
        read(s"${normalized_condition_disease.location}", "Parquet", Map(), None, None)
          .where(col("release_id") === releaseId)
          .where(col("study_id").isin(studyIds:_*)),
      normalized_group.id ->
        read(s"${normalized_group.location}", "Parquet", Map(), None, None)
          .where(col("release_id") === releaseId)
          .where(col("study_id").isin(studyIds:_*)),
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val patientDF = data(normalized_patient.id)

    val hpoTermsConf = conf.sources.find(c => c.id == "hpo_terms").getOrElse(throw new RuntimeException("Hpo Terms Conf not found"))
    val mondoTermsConf = conf.sources.find(c => c.id == "mondo_terms").getOrElse(throw new RuntimeException("Mondo Terms Conf not found"))

    val allHpoTerms = read(s"${hpoTermsConf.location}", "Json", Map(), None, None)
    val allMondoTerms = read(s"${mondoTermsConf.location}", "Json", Map(), None, None)

    val transformedParticipant =
      patientDF
        .addDiagnosisPhenotypes(data(normalized_condition_phenotype.id), data(normalized_condition_disease.id))(allHpoTerms, allMondoTerms)
        .addOutcomes(data(normalized_observation_vital_status.id))
        .addFamily(data(normalized_group.id))
        .withColumnRenamed("gender", "sex")
        .withColumn("karyotype", lit("TODO"))
        .withColumn("down_syndrome_diagnosis", lit("TODO"))
        .withColumn("family_type", lit("TODO"))
        .withColumn("is_proband", lit(false)) // TODO
        .withColumn("age_at_data_collection", lit(111)) // TODO

    transformedParticipant.show(false)
    Map(mainDestination.id -> transformedParticipant)
  }

  override def load(data: Map[String, DataFrame],
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    println(s"COUNT: ${data(mainDestination.id).count()}")
    val dataToLoad = Map(mainDestination.id -> data(mainDestination.id)
      .sortWithinPartitions("fhir_id").toDF())
    super.load(dataToLoad)
  }
}
