package bio.ferlab.fhir.etl

import bio.ferlab.datalake.spark3.elasticsearch.ElasticSearchClient

object PublishTask extends App {
  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(
  esNodes,          // http://localhost:9200
  esPort,           // 9200
  release_id,       // release id
  study_ids,        // study ids separated by ;
  jobType,          // study_centric or participant_centric or file_centric or biospecimen_centric
  ) = args

  val esConfigs = Map(
    "es.index.auto.create" -> "true",
    "es.net.ssl" -> "true",
    "es.net.ssl.cert.allow.self.signed" -> "true",
    "es.nodes" -> esNodes,
    "es.nodes.wan.only" -> "true",
    "es.wan.only" -> "true",
    "spark.es.nodes.wan.only" -> "true",
    "es.port" -> esPort)

  implicit val esClient: ElasticSearchClient = new ElasticSearchClient(esNodes.split(',').head, None, None)

  val studyList = study_ids.split(";")

  studyList.map(studyId => {
    val newIndexName = s"${jobType}_${studyId}_$release_id".toLowerCase
    println(s"Add $newIndexName to alias $jobType")

    val oldIndexName = Publisher.retrievePreviousIndex(jobType, studyId, esNodes.split(',').head)
    oldIndexName.map(old => println(s"Remove $old from alias $jobType"))

    Publisher.publish(jobType, newIndexName, oldIndexName)
  })

}
