package bio.ferlab.fhir.etl.config

import bio.ferlab.datalake.commons.config.BaseETLContext
import mainargs.{ParserForClass, arg}

case class KFRuntimeETLContext(
                                @arg(name = "config", short = 'c', doc = "Config path") path: String,
                                @arg(name = "steps", short = 's', doc = "Steps") steps: String,
                                @arg(name = "app-name", short = 'a', doc = "App name") appName: Option[String]
                              ) extends BaseETLContext[ETLConfiguration](path, steps, appName) {

}

object KFRuntimeETLContext {
  implicit def configParser: ParserForClass[KFRuntimeETLContext] = ParserForClass[KFRuntimeETLContext]
}