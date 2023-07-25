package bio.ferlab.etl.normalized.dataservice

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.net.{InetAddress, InetSocketAddress}
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

object DataService {

  def withDataService[T](routes: Map[String, HttpHandler])(block: String => Future[T])(implicit ec:ExecutionContext): Future[T] = {
    val sunHttpServer: HttpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), 50)
    val url = s"http://localhost:${sunHttpServer.getAddress.getPort}"
    routes.foreach { case (prefix, handler) => sunHttpServer.createContext(prefix, handler) }

    sunHttpServer.start()
    val result = block(url)
    result.onComplete {
      _ => sunHttpServer.stop(0)
    }
    result
  }

}

case class jsonHandler(body: String, statusCode: Int = 200) extends HttpHandler {
  var count = 0

  override def handle(httpExchange: HttpExchange): Unit = {
    count = count + 1
    val bytesBody = body.getBytes
    httpExchange.getResponseHeaders.put("Content-Type", List("application/json").asJava)
    httpExchange.sendResponseHeaders(statusCode, bytesBody.length)
    httpExchange.getResponseBody.write(bytesBody)
    httpExchange.getResponseBody.close()
  }
}

case class jsonHandlerAfterNRetries(body: String, retries: Int, errorStatusCode: Int = 502, errorText: String = "Bad Gateway", statusCode: Int = 200) extends HttpHandler {
  var count = 0

  override def handle(httpExchange: HttpExchange): Unit = {
    if (count < retries - 1) {
      val bytesBody = errorText.getBytes
      httpExchange.getResponseHeaders.put("Content-Type", List("application/text").asJava)
      httpExchange.sendResponseHeaders(errorStatusCode, bytesBody.length)
      httpExchange.getResponseBody.write(bytesBody)
      httpExchange.getResponseBody.close()
    }
    else {
      val bytesBody = body.getBytes
      httpExchange.getResponseHeaders.put("Content-Type", List("application/json").asJava)
      httpExchange.sendResponseHeaders(statusCode, bytesBody.length)
      httpExchange.getResponseBody.write(bytesBody)
      httpExchange.getResponseBody.close()
    }
    count = count + 1

  }
}

