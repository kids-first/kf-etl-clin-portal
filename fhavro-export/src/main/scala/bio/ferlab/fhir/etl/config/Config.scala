package bio.ferlab.fhir.etl.config

import bio.ferlab.fhir.etl.model.Environment
import bio.ferlab.fhir.etl.model.Environment.Environment
import cats.data.ValidatedNel
import cats.implicits._
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.generic.auto._

case class AWSConfig(accessKey: String, secretKey: String, region: String, endpoint: String, pathStyleAccess: Boolean, bucketName: String)

case class KeycloakConfig(cookie: String)

case class FhirConfig(baseUrl: String, schemaPath: String, resources: List[FhirRequest])

case class FhirRequest(`type`: String, schema: String, tag: String, total: Option[String], profile: Option[String], count: Option[Int])

case class Config(awsConfig: AWSConfig,
                  keycloakConfig: KeycloakConfig,
                  fhirConfig: FhirConfig)

object Config {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def readConfiguration(): ValidatedNel[String, Config] = {
    val confResult: Result[Config] = loadConfiguration
    confResult match {
      case Left(errors) =>
        val message = errors.prettyPrint()
        message.invalidNel[Config]
      case Right(conf) => conf.validNel[String]
    }
  }

  private def loadConfiguration: Result[Config] = {
    val environment = readEnvironment
    val manifest = readConfigurationPath
    LOGGER.info(s"Loading configuration in $environment in $manifest")
    ConfigSource.file(s"$manifest/application-$environment.conf").load[Config]
  }

  private def readEnvironment: Environment = {
    Environment.fromString(sys.env.getOrElse("ENV", "dev"))
  }

  private def readConfigurationPath: String = {
    sys.env.getOrElse("CONF", "./src/main/resources").stripSuffix("/").trim
  }
}
