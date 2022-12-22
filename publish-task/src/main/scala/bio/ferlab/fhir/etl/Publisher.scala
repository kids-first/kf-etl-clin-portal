package bio.ferlab.fhir.etl

import bio.ferlab.datalake.spark3.elasticsearch.ElasticSearchClient
import org.apache.hadoop.shaded.org.apache.http.client.methods.HttpGet
import org.apache.hadoop.shaded.org.apache.http.util.EntityUtils
import org.json4s._
import org.json4s.jackson.JsonMethods._

object Publisher {

  def retrievePreviousIndex(alias: String, studyId: String, url: String)(implicit esClient: ElasticSearchClient): Option[String] = {
    val aliasUrl: String => String = aliasName => s"$url/_alias/$aliasName"

    val urlToCall = aliasUrl(alias)
    val response = esClient.http.execute(new HttpGet(urlToCall))
    esClient.http.close()
    if (response.getStatusLine.getStatusCode == 200) {
      val responseBody = EntityUtils.toString(response.getEntity, "UTF-8")
      val aliasIndices = parse(responseBody).values.asInstanceOf[Map[String, Any]]
      return aliasIndices.keys.toList.find(s => s.startsWith(s"${alias}_${studyId.toLowerCase}"))
    }
    None
  }

  def publish(alias: String,
              currentIndex: String,
              previousIndex: Option[String] = None)(implicit esClient: ElasticSearchClient): Unit = {
    esClient.setAlias(add = List(currentIndex), remove = previousIndex.toList, alias)
  }
}
