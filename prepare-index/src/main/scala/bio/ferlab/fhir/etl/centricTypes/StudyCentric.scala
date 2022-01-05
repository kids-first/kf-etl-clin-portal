package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class StudyCentric (batchId: String, loadType: String = "incremental")(implicit configuration: Configuration) extends ETL {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_study_centric")
  val normalized_researchstudy: DatasetConf = conf.getDataset("normalized_researchstudy")
  val storageOutput: StorageConf = conf.getStorage("output")


  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      "normalized_researchstudy" -> read(s"${storageOutput.path}${normalized_researchstudy.path}", "Parquet", Map(), None, None),
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val fileDF = data("normalized_researchstudy")

    fileDF.show(false)
    Map("es_index_study_centric" -> fileDF)
  }

  override def load(data: Map[String, DataFrame],
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    println(s"COUNT: ${data("es_index_study_centric").count()}")
    super.load(data)
  }
}
