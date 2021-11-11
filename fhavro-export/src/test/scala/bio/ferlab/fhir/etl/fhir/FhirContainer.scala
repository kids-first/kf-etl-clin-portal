package bio.ferlab.fhir.etl.fhir

import bio.ferlab.fhir.etl.IContainer
import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait

import java.time.Duration

case object FhirContainer extends IContainer {

  val fhirEnv: Map[String, String] = Map(
    "HAPI_FHIR_GRAPHQL_ENABLED" -> "true",
    "HAPI_DATASOURCE_URL" -> "jdbc:h2:mem:hapi",
    "HAPI_DATASOURCE_DRIVER" -> "org.h2.Driver",
    "HAPI_DATASOURCE_USERNAME" -> "",
    "HAPI_DATASOURCE_PASSWORD" -> "",
    "HAPI_AUTH_ENABLED" -> "false",
    "HAPI_AUDITS_ENABLED" -> "false",
    "HAPI_BIO_ELASTICSEARCH_ENABLED" -> "false",
    "HAPI_LOGGING_INTERCEPTOR_SERVER_ENABLED" -> "false",
    "HAPI_LOGGING_INTERCEPTOR_CLIENT_ENABLED" -> "false",
    "HAPI_VALIDATE_RESPONSES_ENABLED" -> "false",
    "JAVA_OPTS" -> "-Dhibernate.dialect=org.hibernate.dialect.H2Dialect"
  )
  val name = "clin-pipeline-fhir-test"

  val port = 8080
  val container: GenericContainer = GenericContainer(
    "chusj/clin-fhir-server:latest",
    waitStrategy = Wait.forHttp("/").withStartupTimeout(Duration.ofSeconds(120)),
    exposedPorts = Seq(port),
    env = fhirEnv,
    labels = Map("name" -> name)
  )
}
