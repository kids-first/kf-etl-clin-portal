package bio.ferlab.fhir.etl.fhavro

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, Format}
import bio.ferlab.datalake.spark3.transformation.{Custom, Transformation}
import bio.ferlab.fhir.etl.transformations.Transformations.extractionMappings
import org.apache.spark.sql.functions.{col, lit, regexp_extract}
import play.api.libs.json.{JsValue, Json}

import scala.util.matching.Regex

object FhavroToNormalizedMappings {
  val pattern: Regex = "raw_([A-Za-z0-9-_]+)".r

  val idFromUrlRegex = "https://kf-api-fhir-service.kidsfirstdrc.org/[A-Za-z]+/([0-9A-Za-z]+)/_history"

  val INGESTION_TIMESTAMP = "ingested_on"

  def defaultTransformations(releaseId: String): List[Transformation] = {
    List(Custom(_
      .withColumn("fhir_id", regexp_extract(col("id"), idFromUrlRegex, 1))
      .withColumn("release_id", lit(releaseId))
    ))
  }

  def mappings(releaseId: String)(implicit c: Configuration): List[(DatasetConf, DatasetConf, List[Transformation])] = {
    c.sources.filter(s => s.format == Format.AVRO).map(s => {
      val pattern(table) = s.id
      (s, c.getDataset(s"normalized_$table"), defaultTransformations(releaseId) ++ extractionMappings(table))
    }
    )
  }
}
