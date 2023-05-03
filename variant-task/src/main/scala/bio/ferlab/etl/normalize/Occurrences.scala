package bio.ferlab.etl.normalize

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.implicits.SparkUtils.filename
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.time.LocalDateTime

class Occurrences(studyId: String, releaseId: String, vcfV1Pattern: String, vcfV2pattern: String, referenceGenomePath: Option[String])(implicit configuration: Configuration) extends ETLSingleDestination {
  private val enriched_specimen: DatasetConf = conf.getDataset("enriched_specimen")
  private val document_reference: DatasetConf = conf.getDataset("normalized_document_reference")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_snv")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {

    Map(
      "vcf" -> loadVCFs(document_reference.read, studyId, vcfV1Pattern, vcfV2pattern, referenceGenomePath),
      enriched_specimen.id -> enriched_specimen.read.where(col("study_id") === studyId)
    )

  }

  private def getFilesUrl(files: DataFrame, studyId: String, endsWith: String)(implicit spark: SparkSession) = {
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

  private def asV1(inputDf: DataFrame): DataFrame = {
    inputDf
      .withColumn("annotation", firstAnn)
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
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
      .withColumn("annotation", firstAnn)
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
      .drop("annotation", "INFO_ANN")
      .withColumn("genotype", explode(col("genotypes")))
  }

  override def transformSingle(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    val vcf = selectOccurrences(data("vcf"))
    val enrichedSpecimenDF = data(enriched_specimen.id)
    vcf.join(enrichedSpecimenDF, Seq("sample_id"))
      .withRelativesGenotype(Seq("gq", "dp", "info_qd", "filters", "ad_ref", "ad_alt", "ad_total", "ad_ratio", "calls", "affected_status", "zygosity"))
      .withParentalOrigin("parental_origin", col("calls"), col("father_calls"), col("mother_calls"))
      .withGenotypeTransmission("transmission")
      .withCompoundHeterozygous()
  }

  private def selectOccurrences(inputDF: DataFrame): DataFrame = {
    val occurrences = inputDF
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        col("hgvsg"),
        col("variant_class"),
        col("genotype.sampleId") as "sample_id",
        col("genotype.alleleDepths") as "ad",
        col("genotype.depth") as "dp",
        col("genotype.conditionalQuality") as "gq",
        col("genotype.calls") as "calls",
        has_alt,
        is_multi_allelic,
        old_multi_allelic,
        col("qual") as "quality",
        col("INFO_filters")(0) as "filter",
        ac as "info_ac",
        an as "info_an",
        af as "info_af",
        col("INFO_culprit") as "info_culprit",
        col("INFO_SOR") as "info_sor",
        col("INFO_ReadPosRankSum") as "info_read_pos_rank_sum",
        col("INFO_InbreedingCoeff") as "info_inbreeding_coeff",
        col("INFO_PG") as "info_pg",
        col("INFO_FS") as "info_fs",
        col("INFO_DP") as "info_dp",
        optional_info(inputDF, "INFO_DS", "info_ds", "boolean"),
        col("INFO_NEGATIVE_TRAIN_SITE") as "info_info_negative_train_site",
        col("INFO_POSITIVE_TRAIN_SITE") as "info_positive_train_site",
        col("INFO_VQSLOD") as "info_vqslod",
        col("INFO_ClippingRankSum") as "info_clipping_rank_sum",
        col("INFO_RAW_MQ") as "info_raw_mq",
        col("INFO_BaseQRankSum") as "info_base_qrank_sum",
        col("INFO_MLEAF")(0) as "info_mleaf",
        col("INFO_MLEAC")(0) as "info_mleac",
        col("INFO_MQ") as "info_mq",
        col("INFO_QD") as "info_qd",
        col("INFO_DB") as "info_db",
        col("INFO_MQRankSum") as "info_m_qrank_sum",
        optional_info(inputDF, "INFO_loConfDeNovo", "lo_conf_denovo"),
        optional_info(inputDF, "INFO_hiConfDeNovo", "hi_conf_denovo"),
        col("INFO_ExcessHet") as "info_excess_het",
        optional_info(inputDF, "INFO_HaplotypeScore", "info_haplotype_score", "float"),
        col("file_name"),
        lit(studyId) as "study_id",
        lit(releaseId) as "release_id",
        is_normalized
      )
      .withColumn(
        "is_lo_conf_denovo",
        array_contains(functions.split(col("lo_conf_denovo"), ","), col("sample_d"))
      )
      .withColumn(
        "is_hi_conf_denovo",
        array_contains(functions.split(col("hi_conf_denovo"), ","), col("sample_id"))
      )
      .drop("annotation", "lo_conf_denovo", "hi_conf_denovo")
      .withColumn("zygosity", zygosity(col("calls")))
    occurrences
  }

  override def replaceWhere: Option[String] = Some(s"study_id = '$studyId'")

}
