package bio.ferlab.fhir.etl

import bio.ferlab.datalake.spark3.elasticsearch.ElasticSearchClient
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.util.EntityUtils
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.Try

object Publisher {

  def retrievePreviousIndex(alias: String, studyId: String, url: String)(implicit esClient: ElasticSearchClient): Option[String] = {
    val aliasUrl: String => String = aliasName => s"$url/_alias/$aliasName"

    val urlToCall = aliasUrl(alias)
    val response: HttpResponse = esClient.http.execute(new HttpGet(urlToCall))
    esClient.http.close()
    if (response.getStatusLine.getStatusCode == 200) {
      val responseBody = EntityUtils.toString(response.getEntity, "UTF-8")
      val aliasIndices = parse(responseBody).values.asInstanceOf[Map[String, Any]]
      return aliasIndices.keys.toList.filter(s => s.startsWith(s"${alias}_${studyId.toLowerCase}")).headOption
    }
    None
  }

  def publish(alias: String,
              currentIndex: String,
              previousIndex: Option[String] = None)(implicit esClient: ElasticSearchClient): Unit = {
    Try(esClient.setAlias(add = List(currentIndex), remove = List(), alias))
      .foreach(_ => println(s"$currentIndex added to $alias"))
    Try(esClient.setAlias(add = List(), remove = previousIndex.toList, alias))
      .foreach(_ => println(s"${previousIndex.toList.mkString} removed from $alias"))
  }

}
