package bio.ferlab.fhir.etl

import bio.ferlab.fhir.etl.auth.TokenRequest
import bio.ferlab.fhir.etl.fhir.FhirUtils.buildFhirClient
import bio.ferlab.fhir.etl.s3.S3Utils.buildS3Client
import bio.ferlab.fhir.etl.task.FhavroExporter
import ca.uhn.fhir.rest.client.impl.GenericClient
import cats.syntax.all._
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent
import software.amazon.awssdk.services.s3.S3Client

object FhavroExport extends App {
  println(s"ARGS: " + args.mkString("[", ", ", "]"))
  val mArgs = args.filter { x => x.startsWith("--") && x.contains(":") }.map { x => {
    val k :: v :: _ = x.split(":", 2).toList
    (k.replace("--", ""), v)
  }
  }.toMap

  assert(!mArgs.getOrElse("release", "").isBlank, "'release' is needed")
  assert(!mArgs.getOrElse("studies", "").isBlank, "'studies' must not be empty")
  assert(!mArgs.getOrElse("project", "").isBlank, "'project' is needed")

  private val studies = mArgs("studies").split(",").toList.distinct
  private val isFhirVerbose = List("true", "y", "yes").contains(mArgs.getOrElse("verbose", "").toLowerCase)
  withSystemExit {
    withLog {
      withConfiguration(mArgs("project")) { configuration =>
        implicit val s3Client: S3Client = buildS3Client()
        buildFhirClient(
          configuration.fhirConfig.baseUrl,
          TokenRequest(url = configuration.keycloakConfig.tokenUrl, clientId = configuration.keycloakConfig.clientId, clientSecret = configuration.keycloakConfig.clientSecret),
          isFhirVerbose
        ).map { client =>
          implicit val fhirClient: GenericClient = client
          studies.foreach(studyId => {
            val fhavroExporter = new FhavroExporter(configuration.awsConfig.bucketName, mArgs("release"), studyId)
            configuration.fhirConfig.resources.foreach { fhirRequest =>
              val resources: List[BundleEntryComponent] = fhavroExporter.requestExportFor(fhirRequest)
              fhavroExporter.uploadFiles(fhirRequest, resources)
            }
          })
        }.toValidatedNel
      }
    }
  }
}
