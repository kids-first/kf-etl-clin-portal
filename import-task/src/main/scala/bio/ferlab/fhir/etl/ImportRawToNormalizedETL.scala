package bio.ferlab.fhir.etl

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.v2.RawToNormalizedETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.spark3.transformation.Transformation
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import scala.collection.immutable
import scala.util.Try

class ImportRawToNormalizedETL(override val source: DatasetConf,
                               override val mainDestination: DatasetConf,
                               override val transformations: List[Transformation],
                               val releaseId: String,
                               val studyIds: List[String])
                              (override implicit val conf: Configuration) extends RawToNormalizedETL(source, mainDestination, transformations) {

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    log.info(s"extracting: ${source.location}")
    Map(source.id -> source.read
      .where(col("release_id") === releaseId)
      .where(col("study_id").isin(studyIds: _*))
    )
  }

  override def load(data: Map[String, DataFrame],
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    data.map { case (dsid, df) =>
      val ds = conf.getDataset(dsid)
      val nbPartitions = 10
      LoadResolver
        .write(spark, conf)(ds.format -> ds.loadtype)
        .apply(ds, df.coalesce(nbPartitions))
      dsid -> ds.read
    }

  }

}
