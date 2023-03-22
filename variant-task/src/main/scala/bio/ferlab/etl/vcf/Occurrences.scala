package bio.ferlab.etl.vcf

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import org.apache.spark.sql.functions.{array_sort, col, explode, lit, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{annotations, firstAnn, firstCsq, hgvsg, variant_class}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.vcf
import bio.ferlab.datalake.spark3.implicits.SparkUtils.filename

import java.time.LocalDateTime
import scala.collection.Set

/*
- Affected status x
- family id x
- participant id (fhir id?) x
- dbgap_consent_code (biospecimen) x
- proband
- gender x
- sample id or external id? x
 */
class Occurrences(studyId: String, releaseId: String, vcfV1Pattern: String, vcfV2pattern: String)(implicit configuration: Configuration) extends ETLSingleDestination {
  val patient: DatasetConf = conf.getDataset("normalized_patient")
  val family_relationship: DatasetConf = conf.getDataset("normalized_family_relationship")
  val disease: DatasetConf = conf.getDataset("normalized_disease")
  val specimen: DatasetConf = conf.getDataset("normalized_specimen")
  val document_reference: DatasetConf = conf.getDataset("normalized_document_reference")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_snv")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {

    Map(
      "vcf" -> loadVCFs(document_reference.read, studyId, vcfV1Pattern, vcfV2pattern)
    )

  }

  def getFilesUrl(files: DataFrame, studyId: String, endsWith: String)(implicit spark: SparkSession) = {
    import spark.implicits._
    val filesUrl = files.select("s3_url")
      .where(col("study_id") === studyId)
      .as[String].collect()
    filesUrl.distinct.filter(_.endsWith(endsWith)).toSeq

  }

  def loadVCFs(files: DataFrame, studyId: String, vcfV1Pattern: String, vcfV2pattern: String, referenceGenomePath: Option[String] = None)(implicit spark: SparkSession): DataFrame = {
    val v1VCFFilesUrl = getFilesUrl(files, studyId, vcfV1Pattern)
    val v2VCFFilesUrl = getFilesUrl(files, studyId, vcfV2pattern)
    val vcfDF = (v1VCFFilesUrl, v2VCFFilesUrl) match {
      case (Nil, Nil) => throw new IllegalStateException("No VCF files found!")
      case (Nil, genomicFiles) if genomicFiles.nonEmpty => asV1(vcf(genomicFiles.toList, referenceGenomePath))
      case (genomicFiles, Nil) if genomicFiles.nonEmpty => asV2(vcf(genomicFiles.toList, referenceGenomePath))
      case (v1GenomicFiles, v2GenomicFiles) =>
        val v1DF = asV1(vcf(v1GenomicFiles.toList, referenceGenomePath))
        val v2DF = asV2(vcf(v2GenomicFiles.toList, referenceGenomePath))
        v1DF.unionByName(v2DF)

    }
    vcfDF.withColumn("file_name", filename)
  }

  private def asV1(inputDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    inputDf
      .withColumn("annotation", firstAnn)
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
      .drop("annotation", "INFO_ANN")
      .withColumn("INFO_DS", lit(null).cast("boolean"))
      .withColumn("INFO_HaplotypeScore", lit(null).cast("double"))
      .withColumn("genotype", explode($"genotypes"))
      .drop("genotypes")
      .withColumn("INFO_ReadPosRankSum", $"INFO_ReadPosRankSum"(0))
      .withColumn("INFO_ClippingRankSum", $"INFO_ClippingRankSum"(0))
      .withColumn("INFO_RAW_MQ", $"INFO_RAW_MQ"(0))
      .withColumn("INFO_BaseQRankSum", $"INFO_BaseQRankSum"(0))
      .withColumn("INFO_MQRankSum", $"INFO_MQRankSum"(0))
      .withColumn("INFO_ExcessHet", $"INFO_ExcessHet"(0))
      .withColumn(
        "genotype",
        struct(
          $"genotype.sampleId",
          $"genotype.conditionalQuality",
          $"genotype.filters",
          $"genotype.SB",
          $"genotype.alleleDepths",
          $"genotype.PP",
          $"genotype.PID"(0) as "PID",
          $"genotype.phased",
          $"genotype.calls",
          $"genotype.MIN_DP"(0) as "MIN_DP",
          $"genotype.JL",
          $"genotype.PGT"(0) as "PGT",
          $"genotype.phredLikelihoods",
          $"genotype.depth",
          $"genotype.RGQ",
          $"genotype.JP"
        )
      )
  }

  private def asV2(inputDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    inputDf
      .withColumn("annotation", firstAnn)
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
      .drop("annotation", "INFO_ANN")
      .withColumn("genotype", explode(col("genotypes")))
  }

  override def transformSingle(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = ???
}
