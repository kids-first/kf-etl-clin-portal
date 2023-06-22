package bio.ferlab.etl.normalized

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, RepartitionByColumns}
import bio.ferlab.datalake.spark3.genomics.normalized.BaseConsequences
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.etl.normalized.KFVCFUtils.loadVCFs
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class Consequences(studyId: String, vcfPattern: String, referenceGenomePath: Option[String])(implicit configuration: Configuration) extends BaseConsequences(annotationsColumn = annotations, groupByLocus = true) {
  private val document_reference: DatasetConf = conf.getDataset("normalized_document_reference")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_consequences")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      raw_vcf -> loadVCFs(document_reference.read, studyId, vcfPattern, referenceGenomePath)
    )

  }

  override def transformSingle(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    super.transformSingle(data, lastRunDateTime, currentRunDateTime)
      .withColumn("study_id", lit(studyId))
  }

}
