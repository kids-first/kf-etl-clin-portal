package bio.ferlab.fhir.etl.auth

import sttp.client3.{HttpClientSyncBackend, UriContext, basicRequest}
case class TokenRequest(url: String, clientId: String, clientSecret: String, grantType: String = "client_credentials")
object KcTokenHandler {
  def fetch(tr: TokenRequest): Either[String, String] = {
    val be = HttpClientSyncBackend()
    val r = basicRequest
      .body(Map(
        "client_id" -> tr.clientId,
        "client_secret" -> tr.clientSecret,
        "grant_type" -> tr.grantType

      ))
      .post(uri"${tr.url}")
      .send(be)
    be.close()
    r.body.map{ x =>
      val j = ujson.read(x)
      j("access_token").str
    }

  }
}