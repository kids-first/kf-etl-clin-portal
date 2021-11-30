package bio.ferlab.fhir.etl.fhavro

import bio.ferlab.fhir.etl.fhavro.FhavroCustomOperations._
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, Format}
import bio.ferlab.datalake.spark3.transformation.{Custom, Drop, ToDate, Transformation}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, filter, regexp_extract, regexp_replace, to_timestamp}

import scala.util.matching.Regex

object FhavroToNormalizedMappings {
  val pattern: Regex = "raw_([A-Za-z0-9]+)".r

  val idFromUrlRegex = "https://kf-api-fhir-service.kidsfirstdrc.org/[A-Za-z]+/([0-9A-Za-z]+)/_history/2"
  val patternParticipantStudy = "[A-Z][a-z]+-(SD_[0-9A-Za-z]+)-([A-Z]{2}_[0-9A-Za-z]+)"
  val participantSpecimen = "[A-Z][a-z]+/([0-9A-Za-z]+)"


  val URL_US_CORE_RACE = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"

  val URL_US_CORE_ETHNICITY = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"

  val INGESTION_TIMESTAMP = "ingested_on"

  val defaultTransformations: List[Transformation] = List(
    Custom(_
      .withColumn("fhir_id", regexp_extract(col("id"), idFromUrlRegex, 1))
    )
  )

  val patientMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      .withColumn("race", filter(col("extension"), c => c("url") === URL_US_CORE_RACE)(0)("extension")(0)("valueString"))
      .withColumn("ethnicity", filter(col("extension"), c => c("url") === URL_US_CORE_ETHNICITY)(0)("extension")(0)("valueString"))
      .withColumn("external_id", col("identifier")(0)("value"))
      .withColumn("study_id", regexp_extract(col("identifier")(2)("value"), patternParticipantStudy, 1))
      .withColumn("participant_id", regexp_extract(col("identifier")(2)("value"), patternParticipantStudy, 2))
    ),
    Drop("extension", "id", "identifier", "meta")
  )

  val specimenMappings: List[Transformation] = List(
    Custom(_
      .select("*")
      .withColumn("participant_fhir_id",   regexp_extract(col("subject")("reference"), participantSpecimen, 1))
      .withColumn("specimen_id", col("identifier")(0)("value"))
    ),
//    Drop("id", "subject", "identifier")
    Drop()
  )

  val observationMappings: List[Transformation] = List(
    Custom(_
      .select("*")
    ),
    Drop()
  )

  val conditionMappings: List[Transformation] = List(
    Custom(_
      .select("code")
//      .withColumn("study_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 1))
//      .withColumn("diagnosis_id", regexp_extract(col("identifier")(1)("value"), patternParticipantStudy, 2))
      .withColumn("toto", col("code")("coding")(0))
    ),
    Drop()
  )

  val researchsubjectMappings: List[Transformation] = List(
    Custom(_
      .select("*")
    ),
    Drop()
  )

  val groupMappings: List[Transformation] = List(
    Custom(_
      .select("*")
//      .withColumn("kf_id",   regexp_extract(col("id"), idFromUrl, 1))
    ),
    Drop()
  )

  val extractionMappings: Map[String, List[Transformation]] = Map(
    "patient" -> patientMappings,
    "specimen" -> specimenMappings,
    "observation" -> observationMappings,
    "condition" -> conditionMappings,
    "researchsubject" -> researchsubjectMappings,
    "group" -> groupMappings
  )

  def mappings(implicit c: Configuration): List[(DatasetConf, DatasetConf, List[Transformation])] = c.sources.filter(s => s.format == Format.AVRO).map(s =>
    {
      val pattern(table ) = s.id
      (s, c.getDataset(s"normalized_$table"), defaultTransformations ++ extractionMappings(table))
    }
  )
}
