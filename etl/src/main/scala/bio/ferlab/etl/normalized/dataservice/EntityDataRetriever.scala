package bio.ferlab.etl.normalized.dataservice

import akka.actor.{ActorSystem, Scheduler}
import akka.pattern.Patterns.after
import bio.ferlab.etl.normalized.dataservice.EntityDataRetriever.buildUrl
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods
import play.api.libs.ws.StandaloneWSClient

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait DataRetriever{
  def retrieve[T](endpoint: String, data: Seq[T] = Seq.empty[T], retries: Int = 10)(implicit extractor: EntityDataExtractor[T]): Future[Seq[T]]
}

case class EntityDataRetriever(baseUrl: String, filters: Seq[String] = Seq.empty[String])(implicit wsClient: StandaloneWSClient, ec: ExecutionContext, system: ActorSystem) extends DataRetriever {
  private implicit val scheduler: Scheduler = system.scheduler
  implicit val formats: DefaultFormats.type = DefaultFormats
  private lazy val filterQueryString = filters.mkString("&")

  final def retrieve[T](endpoint: String, data: Seq[T] = Seq.empty[T], retries: Int = 10)(implicit extractor: EntityDataExtractor[T]): Future[Seq[T]] = {
    def extractDataset(responseBody: json4s.JValue) = {
      responseBody \ "results" match {
        case JNull | JNothing => Seq.empty
        case obj: JObject =>
          val entity = extractEntity(obj)
          entity match {
            case Some(e) => Seq(e)
            case None => Seq.empty
          }
        case JArray(entities) => extractEntityArray(entities)
        case _ => Seq.empty
      }
    }

    def extractEntityArray(entities: List[JValue]): Seq[T] = {
      entities.map(extractEntity).filter(_.isDefined).map(_.get)
    }

    def extractEntity(entityJson: JValue): Option[T] = {
      val entity = extractor.extract(entityJson)
      Some(entity)
    }

    val url = buildUrl(s"$baseUrl$endpoint", filters)

    wsClient.url(url).withRequestTimeout(Duration(200, "seconds")).withHttpHeaders("User-Agent" -> "PortalETL").get().flatMap { response =>
      if (response.status != 200) {
        if (retries > 0) {
          val delay = (scala.math.pow(2, 10 - retries) * 300).millisecond
          val remainingTry = retries - 1
          println(s"Error ${response.status}, retrying $url in $delay ms, remaining try = $remainingTry)")
          after(delay, scheduler, ec, Future.successful(1)).flatMap { _ =>
            retrieve(endpoint, data, remainingTry)
          }
        }
        else
          Future.failed(new IllegalStateException(s"Impossible to fetch data from $url, got statusCode = ${response.status} and body = ${response.body}"))
      } else {
        val responseBody = JsonMethods.parse(response.body)
        val currentDataset = data ++ extractDataset(responseBody)
        // Retrieve content from "next" URL in links, or return our dataset
        responseBody \ "_links" \ "next" match {
          case JString(next) => retrieve(next, currentDataset)
          case _ => Future.successful(currentDataset)
        }

      }
    }


  }
}

object EntityDataRetriever {
  def buildUrl(uri: String, filters: Seq[String]): String = {
    val parameters = (filters :+ "limit=100").mkString("&")
    if (uri.contains("?")) {
      s"$uri&$parameters"
    } else {
      s"$uri?$parameters"
    }

  }
}
