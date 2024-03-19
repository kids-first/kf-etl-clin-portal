package bio.ferlab.fhir.etl.fhir
import bio.ferlab.fhir.etl.auth.CookieInterceptor
import bio.ferlab.fhir.etl.config.{AWSConfig, Config, FhirConfig, FhirRequest, KeycloakConfig}
import bio.ferlab.fhir.etl.minio.MinioContainer
import bio.ferlab.fhir.etl.task.FhavroExporter
import ca.uhn.fhir.context.{FhirContext, PerformanceOptionsEnum}
import ca.uhn.fhir.parser.IParser
import ca.uhn.fhir.rest.client.api.{IGenericClient, ServerValidationModeEnum}
import org.hl7.fhir.instance.model.api.IIdType
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender
import org.hl7.fhir.r4.model.{CodeableConcept, Coding, Condition, Enumerations, IdType, Meta, Patient, Reference}

import scala.collection.JavaConverters._
import java.time.{LocalDate, ZoneId}
import java.util.Date

trait FhirServer {

  val fhirPort: Int = FhirContainer.startIfNotRunning()
  val fhirBaseUrl = s"http://localhost:$fhirPort/fhir"

  val fhirContext: FhirContext = FhirContext.forR4()

  fhirContext.setPerformanceOptions(PerformanceOptionsEnum.DEFERRED_MODEL_SCANNING)
  fhirContext.getRestfulClientFactory.setServerValidationMode(ServerValidationModeEnum.NEVER)

  val parser: IParser = fhirContext.newJsonParser().setPrettyPrint(true)

  implicit val fhirClient: IGenericClient = fhirContext.newRestfulGenericClient(fhirBaseUrl)

  implicit val fhirConfig: FhirConfig = FhirConfig(null)

  implicit val keycloakConfig: KeycloakConfig = KeycloakConfig("cookie")

  fhirClient.registerInterceptor(new CookieInterceptor("cookie"))

  def loadPatient(lastName: String = "Doe",
                   firstName: String = "John",
                   identifier: String = "PT-000001",
                    tag: String = "SD_ABC")
                  (implicit fhirClient: IGenericClient): Unit = {
    val patient: Patient = new Patient()
    patient.addIdentifier()
      .setSystem("http://terminology.hl7.org/CodeSystem/v2-0203")
      .setValue(identifier)
    patient.setBirthDate(Date.from(LocalDate.of(2000, 12, 21).atStartOfDay(ZoneId.of("UTC")).toInstant))
    patient.setActive(true)
    patient.addName().setFamily(lastName).addGiven(firstName)
    patient.setIdElement(IdType.of(patient.setId(identifier)))
    patient.setGender(Enumerations.AdministrativeGender.MALE)
    patient.setMeta(new Meta().setTag(List(new Coding(null, tag, null)).asJava))

    fhirClient.create()
      .resource(patient)
      .execute()
      .getId
  }

  def loadCondition(system:String = "http://purl.obolibrary.org/obo/mondo.owl", code: String = "0007186", tag: String = "SD_ABC") ={
    val cond = new Condition()
    cond.setSubject(new Reference("Patient/1"))
    cond.setMeta(new Meta().setTag(List(new Coding(null, tag, null)).asJava))
    cond.setCode(new CodeableConcept().addCoding(new Coding(system, code, "")))
    fhirClient.create()
      .resource(cond)
      .execute()
      .getId
  }
}
