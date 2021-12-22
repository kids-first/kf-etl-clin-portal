package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.fhir.etl.common.Utils._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class ParticipantCentric(batchId: String, loadType: String = "incremental")(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("es_index_participant_centric")
  val normalized_researchstudy: DatasetConf = conf.getDataset("normalized_researchstudy")
  val filesExtract: Seq[DatasetConf] = conf.sources.filter(c =>  c.id.contains("normalized"))
  val hpoTermsConf: DatasetConf = conf.getDataset("hpo_terms")
  val inputStorageOutput: StorageConf = conf.getStorage("output")
  val inputHpoTermsStorage: StorageConf = conf.getStorage("hpo_terms")
  val inputMondoTermsStorage: StorageConf = conf.getStorage("mondo_terms")


  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    filesExtract.map(f => f.id -> read(s"${inputStorageOutput.path}${f.path}", "Parquet", Map(), None, None)).toMap
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val patientDF = data("normalized_patient")

    val hpoTermsConf = conf.sources.find(c =>  c.id == "hpo_terms").getOrElse(throw new RuntimeException("Hpo Terms Conf not found"))
    val mondoTermsConf = conf.sources.find(c =>  c.id == "mondo_terms").getOrElse(throw new RuntimeException("Mondo Terms Conf not found"))

    val allHpoTerms = read(s"${inputHpoTermsStorage.path}${hpoTermsConf.path}", "Json", Map(), None, None)
    val allMondoTerms = read(s"${inputMondoTermsStorage.path}${mondoTermsConf.path}", "Json", Map(), None, None)

    val transformedParticipant =
      patientDF
      .addStudy(data("normalized_researchstudy"))
      .addBiospecimen(data("normalized_specimen"))
      .addOutcomes(data("normalized_observation"))
      .addDiagnosisPhenotypes(data("normalized_condition"))(allHpoTerms, allMondoTerms)

    patientDF

  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    println(s"COUNT: ${data.count()}")
    super.load(data
      .repartition(1, col("chromosome"))
      .sortWithinPartitions("start"))
  }
}
