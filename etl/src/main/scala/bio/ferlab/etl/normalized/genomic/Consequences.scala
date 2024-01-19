package bio.ferlab.etl.normalized.genomic

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.genomics.normalized.BaseConsequences
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.etl.normalized.genomic.KFVCFUtils.loadVCFs
import bio.ferlab.fhir.etl.config.StudyConfiguration.defaultStudyConfiguration
import bio.ferlab.fhir.etl.config.{KFRuntimeETLContext, StudyConfiguration}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import java.time.LocalDateTime

case class Consequences(rc: KFRuntimeETLContext, studyId: String, referenceGenomePath: Option[String]) extends BaseConsequences(rc, annotationsColumn = annotations, groupByLocus = true) {
  private val document_reference: DatasetConf = conf.getDataset("normalized_document_reference")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_consequences")

  private val studyConfiguration: StudyConfiguration = rc.config.studies.getOrElse(studyId, defaultStudyConfiguration)

  override def extract(lastRunDateTime: LocalDateTime = minValue,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      raw_vcf -> loadVCFs(document_reference.read, studyConfiguration, studyId, referenceGenomePath)
    )

  }

  override def transformSingle(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime): DataFrame = {
    super.transformSingle(data, lastRunDateTime, currentRunDateTime)
      .withColumn("study_id", lit(studyId))
  }

}
