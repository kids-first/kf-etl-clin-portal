package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.fhir.etl.common.Utils._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class ParticipantCentric(batchId: String, loadType: String = "incremental")(implicit configuration: Configuration) extends ETL {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_participant_centric")
  val simple_participant: DatasetConf = conf.getDataset("simple_participant")
  val es_index_study_centric: DatasetConf = conf.getDataset("es_index_study_centric")
  val normalized_documentreference: DatasetConf = conf.getDataset("normalized_documentreference")
  val storageOutput: StorageConf = conf.getStorage("output")
  val storageEsIndex: StorageConf = conf.getStorage("es_index")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      "simple_participant" -> read(s"${storageEsIndex.path}${simple_participant.path}", "Parquet", Map(), None, None),
      "es_index_study_centric" -> read(s"${storageEsIndex.path}${es_index_study_centric.path}", "Parquet", Map(), None, None),
      "normalized_documentreference" -> read(s"${storageOutput.path}${normalized_documentreference.path}", "Parquet", Map(), None, None),
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val patientDF = data("simple_participant")

    val transformedParticipant =
      patientDF
        .addStudy(data("es_index_study_centric"))
        .addFiles(data("normalized_documentreference")) // TODO add files with their biospecimen (filter by participant)
        .withColumn("study_external_id", col("study")("external_id"))

    transformedParticipant.show(false)
    Map("es_index_participant_centric" -> transformedParticipant)
  }

  override def load(data: Map[String, DataFrame],
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    println(s"COUNT: ${data("es_index_participant_centric").count()}")
    val dataToLoad = Map("es_index_participant_centric" -> data("es_index_participant_centric")
      .repartition(1, col("study_id"))
      .sortWithinPartitions("fhir_id").toDF())
    super.load(dataToLoad)
  }
}
