package bio.ferlab.fhir.etl

import bio.ferlab.datalake.spark3.elasticsearch.ElasticSearchClient

object Publisher {

  def retrievePreviousIndex(alias: String, studyId: String)(implicit esClient: ElasticSearchClient): Option[String] = {
    esClient.getAliasIndices(alias)
      .find(_.startsWith(s"${alias}_${studyId.toLowerCase}"))
  }

  def publish(alias: String,
              currentIndex: String,
              previousIndex: Option[String] = None)(implicit esClient: ElasticSearchClient): Unit = {
    esClient.setAlias(add = List(currentIndex), remove = previousIndex.toList, alias)
  }
}
