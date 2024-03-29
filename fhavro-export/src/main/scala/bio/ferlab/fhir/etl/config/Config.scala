package bio.ferlab.fhir.etl.config

import cats.data.ValidatedNel
import cats.implicits._
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.generic.auto._

case class AWSConfig(bucketName: String)

case class KeycloakConfig(tokenUrl: String, clientId: String, clientSecret: String)

case class FhirConfig(baseUrl: String, resources: List[FhirRequest])

case class FhirRequest(`type`: String, schema: String, total: Option[String], profile: Option[String], entityType: Option[String], count: Option[Int], additionalQueryParam: Option[Map[String, List[String]]])

case class Config(awsConfig: AWSConfig,
                  keycloakConfig: KeycloakConfig,
                  fhirConfig: FhirConfig)

object Config {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def readConfiguration(project: String): ValidatedNel[String, Config] = {
    val confResult: Result[Config] = loadConfiguration(project)
    confResult match {
      case Left(errors) =>
        val message = errors.prettyPrint()
        message.invalidNel[Config]
      case Right(conf) => conf.validNel[String]
    }
  }

  private def loadConfiguration(project: String): Result[Config] = {
    LOGGER.info(s"Loading configuration in $project")
    ConfigSource
      // Usually application-default.conf or it could be application-usf.conf
      .resources(s"application-${project}.conf")
      .load[Config]
  }
}
