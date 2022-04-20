package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.fhir.etl.common.Utils._
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.time.LocalDateTime

class FileCentric(releaseId: String, studyIds: List[String])(implicit configuration: Configuration) extends ETL {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_file_centric")
  val normalized_drs_document_reference: DatasetConf = conf.getDataset("normalized_document_reference")
  val normalized_specimen: DatasetConf = conf.getDataset("normalized_specimen")
  val simple_participant: DatasetConf = conf.getDataset("simple_participant")
  val es_index_study_centric: DatasetConf = conf.getDataset("es_index_study_centric")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Seq(normalized_drs_document_reference, normalized_specimen, simple_participant, es_index_study_centric)
      .map(ds => ds.id -> ds.read.where(col("release_id") === releaseId)
        .where(col("study_id").isin(studyIds: _*))
    ).toMap
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val fileDF = data(normalized_drs_document_reference.id)
    val fhirUrl = spark.conf.get("spark.fhir.server.url")
    val transformedFile =
      fileDF
        .addStudy(data(es_index_study_centric.id))
        .addFileParticipantsWithBiospecimen(data(simple_participant.id), data(normalized_specimen.id))
        .withColumn("fhir_document_reference", concat(lit(fhirUrl), lit("/DocumentReference?identifier="), col("file_id")))
        .withColumn("file_fhir_id", col("fhir_id"))

    Map(mainDestination.id -> transformedFile)
  }

  override def load(data: Map[String, DataFrame],
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val dataToLoad = Map(mainDestination.id -> data(mainDestination.id)
      .coalesce(20).toDF())
    super.load(dataToLoad)
  }
}
