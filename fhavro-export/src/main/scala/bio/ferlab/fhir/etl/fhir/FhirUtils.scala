package bio.ferlab.fhir.etl.fhir

import bio.ferlab.fhir.etl.config.Config
import bio.ferlab.fhir.etl.auth.CookieInterceptor
import ca.uhn.fhir.context.FhirContext
import ca.uhn.fhir.rest.client.impl.GenericClient

object FhirUtils {

  implicit val fhirContext: FhirContext = FhirContext.forR4()

  def buildFhirClient(config: Config): GenericClient =  {
    val fhirClient : GenericClient = fhirContext.getRestfulClientFactory.newGenericClient(s"${config.fhirConfig.baseUrl}").asInstanceOf[GenericClient]
    fhirClient.registerInterceptor(new CookieInterceptor(config.keycloakConfig.cookie))
    fhirClient
  }
}
