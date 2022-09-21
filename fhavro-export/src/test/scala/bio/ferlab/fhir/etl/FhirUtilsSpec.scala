package bio.ferlab.fhir.etl

import bio.ferlab.fhir.etl.fhir.FhirUtils
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class FhirUtilsSpec extends AnyFlatSpec with Matchers {

  "replaceBaseUrl" should "work" in {
    val s = "https://include-api-fhir-service-qa.includedcc.org?_getpages=42a243b1-4211-4ebc-b009-3c59ba68a300&_getpagesoffset=50&_count=50&_bundletype=searchset"

    val replaced = FhirUtils.replaceBaseUrl(s, "http://10.2.9.5:8080")
    replaced shouldBe "http://10.2.9.5:8080/?_getpages=42a243b1-4211-4ebc-b009-3c59ba68a300&_getpagesoffset=50&_count=50&_bundletype=searchset"



  }

}
