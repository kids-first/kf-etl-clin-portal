package bio.ferlab.etl.normalized

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, RepartitionByColumns}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.etl.Constants.columns.{GENES_SYMBOL, TRANSMISSION_MODE}
import bio.ferlab.etl.normalized.KFVCFUtils.loadVCFs
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.time.LocalDateTime

class SNV(studyId: String, releaseId: String, vcfV1Pattern: String, vcfV2pattern: String, referenceGenomePath: Option[String])(implicit configuration: Configuration) extends ETLSingleDestination {
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

  override def transformSingle(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    val vcf = selectOccurrences(data("vcf"))
    val enrichedSpecimenDF = data(enriched_specimen.id)
      .withColumn("mother_id", col("family.mother_id"))
      .withColumn("father_id", col("family.father_id"))
      .drop("family")
    vcf.join(enrichedSpecimenDF, Seq("sample_id"))
      .withRelativesGenotype(Seq("gq", "dp", "info_qd", "filters", "ad_ref", "ad_alt", "ad_total", "ad_ratio", "calls", "affected_status", "zygosity"))
      .withParentalOrigin("parental_origin", col("calls"), col("father_calls"), col("mother_calls"))
      .withGenotypeTransmission(TRANSMISSION_MODE)
    //.withCompoundHeterozygous(patientIdColumnName = "participant_id", geneSymbolsColumnName = "genes_symbol", additionalFilter = Some(array_contains(col("filters"), "PASS")))
  }

  private def selectOccurrences(inputDF: DataFrame): DataFrame = {
    val occurrences = inputDF
      .withColumn("annotation", firstAnn)
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
      .withColumn(GENES_SYMBOL, array_distinct(annotations("symbol")))
      .drop("annotation", "INFO_ANN")
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        col(GENES_SYMBOL),
        col("hgvsg"),
        col("variant_class"),
        col("genotype.sampleId") as "sample_id",
        col("genotype.alleleDepths") as "ad",
        col("genotype.depth") as "dp",
        col("genotype.conditionalQuality") as "gq",
        col("genotype.calls") as "calls",
        array_contains(col("genotype.calls"), 1) as "has_alt",
        is_multi_allelic,
        old_multi_allelic,
        col("qual") as "quality",
        col("INFO_filters") as "filters",
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
        lit(releaseId) as "release_id",
        is_normalized
      )
      .withColumn("ad_ref", col("ad")(0))
      .withColumn("ad_alt", col("ad")(1))
      .withColumn("ad_total", col("ad_ref") + col("ad_alt"))
      .withColumn("ad_ratio", when(col("ad_total") === 0, 0).otherwise(col("ad_alt") / col("ad_total")))
      .drop("ad")
      .withColumn(
        "is_lo_conf_denovo",
        array_contains(functions.split(col("lo_conf_denovo"), ","), col("sample_id"))
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
