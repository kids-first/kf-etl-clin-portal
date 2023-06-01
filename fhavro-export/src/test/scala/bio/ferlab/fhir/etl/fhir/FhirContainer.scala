package bio.ferlab.fhir.etl.fhir

import bio.ferlab.fhir.etl.IContainer
import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait

import java.time.Duration

case object FhirContainer extends IContainer {

  val fhirEnv: Map[String, String] = Map(
    "HAPI_FHIR_GRAPHQL_ENABLED" -> "true",
    "SPRING_DATASOURCE_URL" -> "jdbc:h2:mem:hapi",
    "SPRING_JPA_PROPERTIES_HIBERNATE_DIALECT" -> "org.hibernate.dialect.H2Dialect",
    "SPRING_DATASOURCE_DRIVER_CLASS_NAME" -> "org.h2.Driver",
    "SPRING_DATASOURCE_USERNAME" -> "",
    "SPRING_DATASOURCE_PASSWORD" -> "",
    "HAPI_LOGGING_INTERCEPTOR_SERVER_ENABLED" -> "false",
    "HAPI_LOGGING_INTERCEPTOR_CLIENT_ENABLED" -> "false",
    "HAPI_VALIDATE_RESPONSES_ENABLED" -> "false",
    "HAPI_FHIR_ALLOW_MULTIPLE_DELETE" -> "true",
    "HAPI_FHIR_ALLOW_CASCADING_DELETES" -> "true",
    "HAPI_FHIR_EXPUNGE_ENABLED" -> "true",
    "HAPI_FHIR_REUSE_CACHED_SEARCH_RESULTS_MILLIS" -> "0"
  )
  val name = "clin-pipeline-fhir-test"

  val port = 8080
  val container: GenericContainer = GenericContainer(
    "hapiproject/hapi:v5.4.1",
    waitStrategy = Wait.forHttp("/").withStartupTimeout(Duration.ofSeconds(200)),
    exposedPorts = Seq(port),
    env = fhirEnv,
    labels = Map("name" -> name)
  )
}
