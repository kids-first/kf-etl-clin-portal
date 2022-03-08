package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.fhir.etl.common.Utils._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._

class SimpleParticipant(releaseId: String, studyIds: List[String])(implicit configuration: Configuration) extends ETL {

  override val mainDestination: DatasetConf = conf.getDataset("simple_participant")
  val es_index_study_centric: DatasetConf = conf.getDataset("es_index_study_centric")
  val normalized_patient: DatasetConf = conf.getDataset("normalized_patient")
  val normalized_vital_status: DatasetConf = conf.getDataset("normalized_vital_status")
  val normalized_family_relationship: DatasetConf = conf.getDataset("normalized_family_relationship")
  val normalized_phenotype: DatasetConf = conf.getDataset("normalized_phenotype")
  val normalized_disease: DatasetConf = conf.getDataset("normalized_disease")
  val normalized_group: DatasetConf = conf.getDataset("normalized_group")
  val hpo_terms: DatasetConf = conf.getDataset("hpo_terms")
  val mondo_terms: DatasetConf = conf.getDataset("mondo_terms")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    (Seq(
      es_index_study_centric, normalized_patient, normalized_family_relationship, normalized_phenotype, normalized_disease, normalized_group, normalized_vital_status)
      .map(ds => ds.id -> ds.read.where(col("release_id") === releaseId)
        .where(col("study_id").isin(studyIds: _*))
      ) ++ Seq(
      hpo_terms.id -> hpo_terms.read,
      mondo_terms.id -> mondo_terms.read,
    )).toMap

  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val patientDF = data(normalized_patient.id)

    val transformedParticipant =
      patientDF
        .addStudy(data(es_index_study_centric.id))
        .addDiagnosisPhenotypes(
          data(normalized_phenotype.id),
          data(normalized_disease.id)
        )(data(hpo_terms.id), data(mondo_terms.id))
        .addOutcomes(data(normalized_vital_status.id))
        .addFamily(data(normalized_group.id), data(normalized_family_relationship.id))
        .withColumnRenamed("gender", "sex")
        .withColumn("down_syndrome_status", downsyndromeStatusExtract(col("diagnosis.source_text")))
        .withColumn("down_syndrome_diagnosis", lit("TODO"))
        .withColumn("is_proband", lit(false)) // TODO
        .withColumn("age_at_data_collection", lit(111)) // TODO
        .withColumn("study_external_id", col("study")("external_id"))

    transformedParticipant.show(false)
    Map(mainDestination.id -> transformedParticipant)
  }

  override def load(data: Map[String, DataFrame],
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val dataToLoad = Map(mainDestination.id -> data(mainDestination.id)
      .sortWithinPartitions("fhir_id").toDF())

    super.load(dataToLoad)
  }
}
