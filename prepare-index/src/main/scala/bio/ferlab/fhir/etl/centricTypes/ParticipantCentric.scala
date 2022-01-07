package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.fhir.etl.common.Utils._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class ParticipantCentric(batchId: String, loadType: String = "incremental")(implicit configuration: Configuration) extends ETL {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_participant_centric")
  val common_participant: DatasetConf = conf.getDataset("common_participant")
  val normalized_documentreference: DatasetConf = conf.getDataset("normalized_documentreference")
  val storageOutput: StorageConf = conf.getStorage("output")
  val storageEsIndex: StorageConf = conf.getStorage("es_index")



  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      "common_participant" -> read(s"${storageEsIndex.path}${common_participant.path}", "Parquet", Map(), None, None),
      "normalized_documentreference" -> read(s"${storageOutput.path}${normalized_documentreference.path}", "Parquet", Map(), None, None),
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val patientDF = data("common_participant")

    val transformedParticipant =
      patientDF
        .addFiles(data("normalized_documentreference"))

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
