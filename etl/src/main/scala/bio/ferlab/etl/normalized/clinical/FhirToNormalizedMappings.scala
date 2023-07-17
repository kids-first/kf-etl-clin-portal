package bio.ferlab.etl.normalized.clinical

import bio.ferlab.datalake.commons.config.{DatasetConf, Format}
import bio.ferlab.datalake.spark3.transformation.{Custom, Transformation}
import bio.ferlab.etl.normalized.clinical.Transformations.extractionMappingsFor
import bio.ferlab.fhir.etl.config.ETLConfiguration
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, regexp_extract}

import scala.util.matching.Regex

object FhirToNormalizedMappings {
  val pattern: Regex = "raw_([A-Za-z0-9-_]+)".r

  def generateFhirIdColumValueFromIdColum(): Column =
    regexp_extract(col("id"), "^https?:\\/\\/.+\\/([A-Za-z0-9\\-\\.]{1,64})\\/_history.*$", 1)

  def defaultTransformations(releaseId: String): List[Transformation] = {
    List(Custom(_
      .withColumn("fhir_id", generateFhirIdColumValueFromIdColum())
      .withColumn("release_id", lit(releaseId))
      .withColumn("full_url", col("fullUrl"))
    ))
  }

  def mappings(releaseId: String)(implicit c: ETLConfiguration): List[(DatasetConf, DatasetConf, List[Transformation])] = {
    c.sources.filter(s => s.format == Format.AVRO).map(s => {
      val pattern(table) = s.id
      val mappings = extractionMappingsFor(c.isFlatSpecimenModel)
      (s, c.getDataset(s"normalized_$table"), defaultTransformations(releaseId) ++ mappings.getOrElse(table, Nil))
    }
    )
  }
}
