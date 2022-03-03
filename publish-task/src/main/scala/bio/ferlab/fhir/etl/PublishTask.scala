package bio.ferlab.fhir.etl

import bio.ferlab.datalake.spark3.elasticsearch.ElasticSearchClient

object PublishTask extends App {
  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(
  esNodes, // http://localhost:9200
  esPort, // 9200
  release_id, // release id
  study_ids, // study ids separated by ,
  jobTypes, // study_centric or participant_centric or file_centric or biospecimen_centric or all. can be multivalue spearate by ,
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

  val studyList = study_ids.split(",")

  val jobs = if (jobTypes == "all") Seq("biospecimen_centric", "participant_centric", "study_centric", "file_centric") else jobTypes.split(",").toSeq
  jobs.foreach { job =>
    studyList.foreach(studyId => {
      val newIndexName = s"${job}_${studyId}_$release_id".toLowerCase
      println(s"Add $newIndexName to alias $job")

      val oldIndexName = Publisher.retrievePreviousIndex(job, studyId, esNodes.split(',').head)
      oldIndexName.foreach(old => println(s"Remove $old from alias $job"))

      Publisher.publish(job, newIndexName, oldIndexName)
    })
  }

}
