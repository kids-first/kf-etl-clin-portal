package bio.ferlab.etl.published.clinical

import bio.ferlab.datalake.spark3.elasticsearch.ElasticSearchClient
import bio.ferlab.etl.published.PublishUtils
import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}

object RunPublishClinical extends App {
  private val log = LoggerFactory.getLogger("publish")
  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(
  esNodes, // http://localhost:9200
  esPort, // 9200
  release_id, // release id
  study_ids, // study ids separated by ,
  jobTypes, // study_centric or participant_centric or file_centric or biospecimen_centric or all. can be multivalue spearate by ,
  ) = args

  private val esUsername = sys.env.get("ES_USERNAME")
  private val esPassword = sys.env.get("ES_PASSWORD")

  implicit val esClient: ElasticSearchClient = new ElasticSearchClient(esNodes.split(',').head, esUsername, esPassword)

  private val studyList = study_ids.split(",")

  private val jobs = if (jobTypes == "all") Seq("biospecimen_centric", "participant_centric", "study_centric", "file_centric") else jobTypes.split(",").toSeq
  private val results: Seq[Result[Unit]] = jobs.flatMap { job =>
    studyList.map(studyId => Result(job, studyId, Try {
      val newIndexName = s"${job}_${studyId}_$release_id".toLowerCase
      println(s"Add $newIndexName to alias $job")

      val oldIndexName = PublishUtils.retrievePreviousIndex(job, studyId)
      oldIndexName.foreach(old => println(s"Remove $old from alias $job"))

      PublishUtils.publish(job, newIndexName, oldIndexName)
    })
    )
  }

  if (results.forall(_.t.isSuccess)) {
    System.exit(0)
  } else {
    results.collect { case Result(job, studyId, Failure(exception)) =>
      log.error(s"An error occur for study $studyId, job $job", exception)
    }
    System.exit(-1)
  }

  private case class Result[T](job: String, studyId: String, t: Try[T])
}
