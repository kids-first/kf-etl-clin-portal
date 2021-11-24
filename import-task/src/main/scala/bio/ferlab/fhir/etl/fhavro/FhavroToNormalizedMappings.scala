package bio.ferlab.fhir.etl.fhavro

import bio.ferlab.fhir.etl.fhavro.FhavroCustomOperations._
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.transformation.{Custom, Drop, ToDate, Transformation}

object FhavroToNormalizedMappings {
  val INGESTION_TIMESTAMP = "ingested_on"

  val defaultTransformations: List[Transformation] = List()

  val patientMappings: List[Transformation] = List(
    // TODO Map Patient to Participant
  )

  def mappings(implicit c: Configuration): List[(DatasetConf, DatasetConf, List[Transformation])] = List(
    (c.getDataset("raw_patient"), c.getDataset("normalized_patient"), defaultTransformations ++ patientMappings)
  )
}
