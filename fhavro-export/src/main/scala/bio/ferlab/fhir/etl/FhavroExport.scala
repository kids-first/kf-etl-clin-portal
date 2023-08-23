package bio.ferlab.fhir.etl

import bio.ferlab.fhir.etl.fhir.FhirUtils.buildFhirClient
import bio.ferlab.fhir.etl.s3.S3Utils.buildS3Client
import bio.ferlab.fhir.etl.task.FhavroExporter
import ca.uhn.fhir.rest.client.impl.GenericClient
import cats.implicits.catsSyntaxValidatedId
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent
import software.amazon.awssdk.services.s3.S3Client

object FhavroExport extends App {
  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  private val argsPositions = Map(
    "releaseId" -> 0,
    "studyIds" -> 1,
    "project" -> 2,
    "verbose" -> 3,
  )

  private def extractVerboseParamOrDefault(array: Array[String]): Boolean = {
    def isVerbose(raw: String): Boolean = {
      raw != null && List("yes", "true", "y").contains(raw.toLowerCase())
    }
    val argsLengthWhenVerbose = 4
    val hasVerboseArg = array.length == argsLengthWhenVerbose
    if (hasVerboseArg) isVerbose(array(argsPositions("verbose"))) else false
  }

  val releaseId = args(argsPositions("releaseId"))
  val studyIds = args(argsPositions("studyIds"))
  val project = args(argsPositions("project"))

  private val studyList = studyIds.split(",").toList

  studyList.foreach(studyId => {
    withSystemExit {
      withLog {
        withConfiguration(project) { configuration =>
          implicit val s3Client: S3Client = buildS3Client()
          implicit val fhirClient: GenericClient = buildFhirClient(configuration, extractVerboseParamOrDefault(args))

          val fhavroExporter = new FhavroExporter(configuration.awsConfig.bucketName, releaseId, studyId)

          configuration.fhirConfig.resources.foreach { fhirRequest =>
            val resources: List[BundleEntryComponent] = fhavroExporter.requestExportFor(fhirRequest)
            fhavroExporter.uploadFiles(fhirRequest, resources)
          }

          "Success!".validNel[String]
        }
      }
    }
  })
}
