package bio.ferlab.etl.published

import bio.ferlab.datalake.spark3.elasticsearch.ElasticSearchClient

object PublishUtils {

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
