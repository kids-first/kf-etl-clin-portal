package bio.ferlab.etl.normalized.dataservice

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api.libs.ws.ahc.StandaloneAhcWSClient

class DefaultContext extends AutoCloseable {
  private var system: ActorSystem = _
  private var materializer: ActorMaterializer = _
  private var wsClientMutable: StandaloneAhcWSClient = _

  object implicits {
    implicit def wsClient: StandaloneAhcWSClient = wsClientMutable



    implicit def actorSystem: ActorSystem = system
  }

  private def init(): Unit = {
    system = ActorSystem()
    materializer = ActorMaterializer()(system)
    wsClientMutable = StandaloneAhcWSClient()(materializer)
  }



  def close(): Unit = {
    println("Close default context")
    wsClientMutable.close()
    system.terminate()
  }

  sys.addShutdownHook(close())
}

object DefaultContext {
  def withContext[T](f: DefaultContext => T): T = {
    val context = new DefaultContext()
    try {
      context.init()
      f(context)
    } finally {
      context.close()
    }
  }

}
