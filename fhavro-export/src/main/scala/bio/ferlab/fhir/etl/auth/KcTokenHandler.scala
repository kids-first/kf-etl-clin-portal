package bio.ferlab.fhir.etl.auth

import sttp.client3.{HttpClientSyncBackend, UriContext, basicRequest}
case class TokenRequest(kcUrl: String, clientId: String, clientSecret: String, grantType: String = "client_credentials", realm: String)
object KcTokenHandler {
  def fetch(tr: TokenRequest): Either[String, String] = {
    val kcUrl = if (tr.kcUrl.endsWith("/")) tr.kcUrl.dropRight(1) else tr.kcUrl
    val be = HttpClientSyncBackend()
    val r = basicRequest
      .body(Map(
        "client_id" -> tr.clientId,
        "client_secret" -> tr.clientSecret,
        "grant_type" -> tr.grantType

      ))
      .post(uri"${kcUrl}/realms/${tr.realm}/protocol/openid-connect/token")
      .send(be)
    be.close()
    r.body.map{ x =>
      val j = ujson.read(x)
      j("access_token").str
    }

  }
}
