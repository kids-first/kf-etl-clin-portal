package bio.ferlab.etl.normalized

import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.implicits.SparkUtils.filename
import bio.ferlab.etl.Constants.columns.GENES_SYMBOL
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object KFVCFUtils {

  private def getFilesUrl(files: DataFrame, studyId: String, endsWith: String)(implicit spark: SparkSession) = {
    import spark.implicits._
    val filesUrl = files.select("s3_url")
      .where(col("study_id") === studyId and col("s3_url").isNotNull)
      .distinct()
      .as[String].collect()
    if (filesUrl == null) Nil else
      filesUrl
        .collect { case s if s != null && s.endsWith(endsWith) => s.replace("s3://", "s3a://") }
        .toSeq

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

  private def asV1(inputDf: DataFrame): DataFrame = {
    inputDf
      .drop("annotation", "INFO_ANN")
      .withColumn("INFO_DS", lit(null).cast("boolean"))
      .withColumn("INFO_HaplotypeScore", lit(null).cast("double"))
      .withColumn("genotype", explode(col("genotypes")))
      .drop("genotypes")
      .withColumn("INFO_ReadPosRankSum", col("INFO_ReadPosRankSum")(0))
      .withColumn("INFO_ClippingRankSum", col("INFO_ClippingRankSum")(0))
      .withColumn("INFO_RAW_MQ", col("INFO_RAW_MQ")(0))
      .withColumn("INFO_BaseQRankSum", col("INFO_BaseQRankSum")(0))
      .withColumn("INFO_MQRankSum", col("INFO_MQRankSum")(0))
      .withColumn("INFO_ExcessHet", col("INFO_ExcessHet")(0))
      .withColumn(
        "genotype",
        struct(
          col("genotype.sampleId"),
          col("genotype.conditionalQuality"),
          col("genotype.filters"),
          col("genotype.SB"),
          col("genotype.alleleDepths"),
          col("genotype.PP"),
          col("genotype.PID")(0) as "PID",
          col("genotype.phased"),
          col("genotype.calls"),
          col("genotype.MIN_DP")(0) as "MIN_DP",
          col("genotype.JL"),
          col("genotype.PGT")(0) as "PGT",
          col("genotype.phredLikelihoods"),
          col("genotype.depth"),
          col("genotype.RGQ"),
          col("genotype.JP")
        )
      )
  }

  private def asV2(inputDf: DataFrame): DataFrame = {
    inputDf
      .withColumn("genotype", explode(col("genotypes")))
  }

}
