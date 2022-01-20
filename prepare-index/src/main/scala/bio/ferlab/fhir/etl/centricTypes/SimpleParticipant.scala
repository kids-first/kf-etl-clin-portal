package bio.ferlab.fhir.etl.centricTypes

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.fhir.etl.common.Utils._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class SimpleParticipant(batchId: String, loadType: String = "incremental")(implicit configuration: Configuration) extends ETL {

  override val mainDestination: DatasetConf = conf.getDataset("simple_participant")
  val filesExtract: Seq[DatasetConf] = conf.sources.filter(c => c.id.contains("normalized"))
  val hpoTermsConf: DatasetConf = conf.getDataset("hpo_terms")
  val storageOutput: StorageConf = conf.getStorage("output")
  val storageHpoTerms: StorageConf = conf.getStorage("hpo_terms")
  val storageMondoTerms: StorageConf = conf.getStorage("mondo_terms")


  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    filesExtract.map(f => f.id -> read(s"${storageOutput.path}${f.path}", "Parquet", Map(), None, None)).toMap
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val patientDF = data("normalized_patient")

    val hpoTermsConf = conf.sources.find(c => c.id == "hpo_terms").getOrElse(throw new RuntimeException("Hpo Terms Conf not found"))
    val mondoTermsConf = conf.sources.find(c => c.id == "mondo_terms").getOrElse(throw new RuntimeException("Mondo Terms Conf not found"))

    val allHpoTerms = read(s"${storageHpoTerms.path}${hpoTermsConf.path}", "Json", Map(), None, None)
    val allMondoTerms = read(s"${storageMondoTerms.path}${mondoTermsConf.path}", "Json", Map(), None, None)

    val transformedParticipant =
      patientDF
        .addDiagnosisPhenotypes(data("normalized_condition"))(allHpoTerms, allMondoTerms)
    //        .addOutcomes(data("normalized_observation"))
        .addFamily(data("normalized_group"))
        .withColumnRenamed("gender", "sex")
        .withColumn("karyotype", lit("TODO"))
        .withColumn("down_syndrome_diagnosis", lit("TODO"))
        .withColumn("family_type", lit("TODO"))
        .withColumn("is_proband", lit("TODO"))
        .withColumn("age_at_data_collection", lit("TODO"))

    transformedParticipant.show(false)
    Map("simple_participant" -> transformedParticipant)
  }

  override def load(data: Map[String, DataFrame],
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    println(s"COUNT: ${data("simple_participant").count()}")
    val dataToLoad = Map("simple_participant" -> data("simple_participant")
      .repartition(1, col("study_id"))
      .sortWithinPartitions("fhir_id").toDF())
    super.load(dataToLoad)
  }
}
