package bio.ferlab.etl.normalized.dataservice

import akka.actor.ActorSystem
import bio.ferlab.datalake.commons.config.{Coalesce, Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.etl.normalized.dataservice.model.{ESequencingCenter, ESequencingExperiment, ESequencingExperimentGenomicFile}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.runtime.universe.TypeTag

class DataserviceExportETL(val releaseId: String,
                           val studyIds: List[String],
                           val retriever: DataRetriever)
                          (implicit override val conf: Configuration,
                           ec: ExecutionContext) extends ETL {
  val normalized_sequencing_experiment: DatasetConf = conf.getDataset("normalized_sequencing_experiment")
  override val mainDestination: DatasetConf = normalized_sequencing_experiment
  val normalized_sequencing_experiment_genomic_file: DatasetConf = conf.getDataset("normalized_sequencing_experiment_genomic_file")
  val normalized_sequencing_center: DatasetConf = conf.getDataset("normalized_sequencing_center")

  def makeEndpointSeq(studyId: String, path: String): String =
    s"$path?study_id=$studyId"

  override def extract(lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    val futureDataframes = for {
      sequencingExperiments <- retrieveForStudies[ESequencingExperiment]("/sequencing-experiments")
      sequencingExperimentGenomicFiles <- retrieveForStudies[ESequencingExperimentGenomicFile]("/sequencing-experiment-genomic-files")
      sequencingCenters <- retrieveForStudies[ESequencingCenter]("/sequencing-centers")
    } yield {
      Map(
        normalized_sequencing_experiment.id -> sequencingExperiments,
        normalized_sequencing_experiment_genomic_file.id -> sequencingExperimentGenomicFiles,
        normalized_sequencing_center.id -> sequencingCenters
      )
    }
    Await.result(futureDataframes, Duration.Inf)

  }

  def retrieveForStudies[T <: Product : TypeTag](baseUrl: String)(implicit spark: SparkSession, extractor: EntityDataExtractor[T]): Future[DataFrame] = Future.traverse(studyIds) { studyId =>
    val endpoint = makeEndpointSeq(studyId, baseUrl)
    val studyEntitiesF: Future[Seq[T]] =
      retriever.retrieve[T](endpoint)
    studyEntitiesF.map(studyEntities =>
      spark.createDataFrame(studyEntities)
        .withColumn("study_id", lit(studyId))
    )
  }.map { dataframes => dataframes.reduce((d1, d2) => d1.unionByName(d2, allowMissingColumns = true))
    .withColumn("release_id", lit(releaseId))
  }

  override def transform(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = data

  override val defaultRepartition: DataFrame => DataFrame = Coalesce(5)
}
