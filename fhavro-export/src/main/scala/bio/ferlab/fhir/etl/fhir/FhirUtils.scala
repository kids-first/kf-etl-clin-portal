package bio.ferlab.fhir.etl.fhir

import bio.ferlab.fhir.etl.auth.{KcTokenHandler, TokenRequest}
import ca.uhn.fhir.context.FhirContext
import ca.uhn.fhir.rest.client.api.IRestfulClientFactory
import ca.uhn.fhir.rest.client.impl.GenericClient
import ca.uhn.fhir.rest.client.interceptor.{BearerTokenAuthInterceptor, LoggingInterceptor}
import org.apache.http.HttpHost
import org.apache.http.client.utils.URIUtils
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI

object FhirUtils {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  private def isUpgradedServer(url: String) = {
    //https://github.com/kids-first/kf-api-fhir-service/blob/eacdf0771ec87da1428b58cf915b78106cc3d801/README.md?plain=1#L19
    List(
      "https://kf-api-fhir-service-upgrade-dev.kf-strides.org",
      "https://kf-api-fhir-service-upgrade-qa.kf-strides.org",
      "https://kf-api-fhir-service-upgrade.kf-strides.org",
      "https://include-api-fhir-service-upgrade-dev.includedcc.org",
      "https://include-api-fhir-service-upgrade-qa.includedcc.org",
      "https://include-api-fhir-service-upgrade.includedcc.org",
    ).contains(url)
  }

  def buildFhirClient(fhir_url: String, tr: TokenRequest, verbose: Boolean): Either[String, GenericClient] = {
    val loggingInterceptor = new LoggingInterceptor()
    val ctx = FhirContext.forR4().getRestfulClientFactory
    ctx.setSocketTimeout(2 * IRestfulClientFactory.DEFAULT_SOCKET_TIMEOUT)
    val fhirClient: GenericClient = ctx.newGenericClient(fhir_url).asInstanceOf[GenericClient]
    if (verbose) {
      loggingInterceptor.setLogRequestSummary(true)
      loggingInterceptor.setLogRequestBody(true)
      fhirClient.registerInterceptor(loggingInterceptor);
    }
    LOGGER.info(s"isUpgradedServer? ${isUpgradedServer(fhir_url)}") //TODO: remove
    if (isUpgradedServer(fhir_url)) {
      LOGGER.info(s"About to fetch access token") //TODO: remove
      LOGGER.info(s"Token Request is: url=${tr.url} client-id=${tr.clientId} grant-type=${tr.grantType} client-secret=${tr.clientSecret}") //TODO: remove
      val token: Either[String, String] = KcTokenHandler.fetch(
        tr
      )

      if (token.isLeft) { //TODO: remove
        LOGGER.info(s"Error fetching token. Got ${token.left.getOrElse("no value")}")
      }

      token.map { s =>
        LOGGER.info(s"Using Token ${s}") //TODO: remove
        fhirClient.registerInterceptor(new BearerTokenAuthInterceptor(s))
        fhirClient
      }
    } else {
      Right(fhirClient)
    }
  }

  def replaceBaseUrl(url: String, replaceHost: String): String = {
    val replaceHostUri = new URI(replaceHost)
    URIUtils.rewriteURI(URI.create(url), new HttpHost(replaceHostUri.getHost, replaceHostUri.getPort, replaceHostUri.getScheme)).toString
  }
}
