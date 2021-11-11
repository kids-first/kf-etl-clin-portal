import bio.ferlab.fhir.Fhavro
import bio.ferlab.fhir.etl.config.{Config, FhirRequest}
import bio.ferlab.fhir.etl.fhir.FhirServerSuite
import bio.ferlab.fhir.etl.task.FhavroExporter
import bio.ferlab.fhir.schema.repository.SchemaMode
import org.scalatest.{FlatSpec, Matchers}

class FhavroExporterTest extends FlatSpec with FhirServerSuite with Matchers {

  "requestExportFor" should "return a List of Fhir Resource by tag" in {
    val fhirRequest = FhirRequest("Patient", "kfdrc-patient", "SD_001", None, None, None)
    val resources = new FhavroExporter(Config(awsConfig, keycloakConfig, fhirConfig)).requestExportFor(fhirRequest)
    resources.length shouldBe 2
  }

  "convertFileContentToGenericRecord" should "return a list of Generic Record" in {
    val fhirRequest = FhirRequest("Patient", "kfdrc-patient", "SD_001", None, None, None)
    val fhavroExporter = new FhavroExporter(Config(awsConfig, keycloakConfig, fhirConfig))
    val resources = fhavroExporter.requestExportFor(fhirRequest)
    val schema = Fhavro.loadSchema("./src/test/resources/schema/patient.avsc", SchemaMode.ADVANCED)
    fhavroExporter.convertResourcesToGenericRecords(schema, resources).length shouldBe 2
  }
}
