package bio.ferlab.fhir.etl

import bio.ferlab.fhir.etl.task.FhavroExporter
import cats.implicits.catsSyntaxValidatedId

object FhavroExport extends App {
  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(releaseId, studyIds) = args

  val studyList = studyIds.split(";").toList

  studyList.foreach(studyId => {
    withSystemExit {
      withLog {
        withConfiguration { configuration =>

          val fhavroExporter = new FhavroExporter(configuration, releaseId, studyId)

          configuration.fhirConfig.resources.foreach(fhirRequest => {

            val resources = fhavroExporter.requestExportFor(fhirRequest)

            fhavroExporter.uploadFiles(fhirRequest, configuration.fhirConfig.schemaPath, resources, releaseId, studyId)
          }).valid[String].valid
        }
      }
    }
  })
}
