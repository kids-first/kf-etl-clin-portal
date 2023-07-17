package bio.ferlab.etl.normalized.dataservice

import bio.ferlab.etl.normalized.dataservice.EntityDataExtractor.seqExp.getIdFromLink
import bio.ferlab.etl.normalized.dataservice.model.{ESequencingCenter, ESequencingExperiment, ESequencingExperimentGenomicFile}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JString, JValue}

trait EntityDataExtractor[T] {
  def extract(json: JValue): T

  def getIdFromLink(linkName: String, json: JValue): Option[String] = {
    json \ "_links" \ linkName match {
      case JString(endpoint) =>
        Some(endpoint.substring(endpoint.lastIndexOf('/') + 1))
      case _ => None
    }
  }

}

object EntityDataExtractor {
  private implicit val formats: DefaultFormats.type = DefaultFormats

  implicit val seqExp: EntityDataExtractor[ESequencingExperiment] =
    (json: JValue) => json.extract[ESequencingExperiment]

  implicit val seqExpGenomicFile
      : EntityDataExtractor[ESequencingExperimentGenomicFile] =
    (json: JValue) => {
      val entity = json.extract[ESequencingExperimentGenomicFile]
      entity.copy(
        sequencing_experiment = getIdFromLink("sequencing_experiment", json),
        genomic_file = getIdFromLink("genomic_file", json)
      )
    }

  implicit val sequencingCenters: EntityDataExtractor[ESequencingCenter] =
    (json: JValue) => json.extract[ESequencingCenter]
}
