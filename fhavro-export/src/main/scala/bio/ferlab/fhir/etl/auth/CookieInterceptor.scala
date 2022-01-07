package bio.ferlab.fhir.etl.auth

import ca.uhn.fhir.rest.client.api.{IClientInterceptor, IHttpRequest, IHttpResponse}

class CookieInterceptor(cookie: String) extends IClientInterceptor {

  override def interceptRequest(request: IHttpRequest): Unit = {
    request.addHeader("Cookie", cookie)
  }

  override def interceptResponse(theResponse: IHttpResponse): Unit = {}
}
