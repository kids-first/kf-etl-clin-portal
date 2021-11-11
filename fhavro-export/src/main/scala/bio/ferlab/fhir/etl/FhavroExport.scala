package bio.ferlab.fhir.etl

import bio.ferlab.fhir.etl.task.FhavroExporter
import cats.implicits.catsSyntaxValidatedId

object FhavroExport extends App {
  withSystemExit {
    withLog {
      withConfiguration { configuration =>
        val fhavroExporter = new FhavroExporter(configuration)

        configuration.fhirConfig.resources.foreach(fhirRequest => {

          val resources = fhavroExporter.requestExportFor(fhirRequest)

          fhavroExporter.uploadFiles(fhirRequest, configuration.fhirConfig.schemaPath, resources)
        }).valid[String].valid
      }
    }
  }
}
