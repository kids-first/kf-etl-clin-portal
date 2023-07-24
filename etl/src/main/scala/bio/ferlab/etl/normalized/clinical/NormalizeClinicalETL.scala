package bio.ferlab.etl.normalized.clinical

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.etl.v3.TransformationsETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.transformation.Transformation
import bio.ferlab.fhir.etl.config.KFRuntimeETLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import java.time.LocalDateTime

class NormalizeClinicalETL(val rc: KFRuntimeETLContext,
                           override val source: DatasetConf,
                           override val mainDestination: DatasetConf,
                           override val transformations: List[Transformation],
                           val releaseId: String,
                           val studyIds: List[String]) extends TransformationsETL(rc, source, mainDestination, transformations) {

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime): Map[String, DataFrame] = {
    log.info(s"extracting: ${source.location}")
    Map(source.id -> source.read
      .where(col("release_id") === releaseId)
      .where(col("study_id").isin(studyIds: _*))
    )
  }

  override def replaceWhere: Option[String] = Some(s"study_id in (${studyIds.map(s => s"'$s'").mkString(", ")})")
}
