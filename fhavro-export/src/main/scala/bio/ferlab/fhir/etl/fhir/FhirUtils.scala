package bio.ferlab.fhir.etl.fhir

import bio.ferlab.fhir.etl.auth.CookieInterceptor
import bio.ferlab.fhir.etl.config.Config
import ca.uhn.fhir.context.FhirContext
import ca.uhn.fhir.rest.client.impl.GenericClient
import ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor
import org.apache.http.HttpHost
import org.apache.http.client.utils.URIUtils

import java.net.URI

object FhirUtils {

  implicit val fhirContext: FhirContext = FhirContext.forR4()

  def buildFhirClient(config: Config, verbose: Boolean): GenericClient = {
    val loggingInterceptor = new LoggingInterceptor()
    val fhirClient: GenericClient = fhirContext.getRestfulClientFactory.newGenericClient(s"${config.fhirConfig.baseUrl}").asInstanceOf[GenericClient]
    println(s"buildFhirClient boolean verbose=${verbose}")//TODO TEMP DEBUG
    if (verbose) {
      println(s"buildFhirClient in branch verbose=true")//TODO TEMP DEBUG
      loggingInterceptor.setLogRequestSummary(true)
      loggingInterceptor.setLogRequestBody(true)
      fhirClient.registerInterceptor(loggingInterceptor);
    }
    config.keycloakConfig.foreach(kc => fhirClient.registerInterceptor(new CookieInterceptor(kc.cookie)))
    fhirClient
  }

  def replaceBaseUrl(url: String, replaceHost: String) = {
    val replaceHostUri = new URI(replaceHost)
    URIUtils.rewriteURI(URI.create(url), new HttpHost(replaceHostUri.getHost, replaceHostUri.getPort, replaceHostUri.getScheme)).toString
  }
}
