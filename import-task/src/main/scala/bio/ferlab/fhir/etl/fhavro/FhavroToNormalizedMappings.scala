package bio.ferlab.fhir.etl.fhavro

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, Format}
import bio.ferlab.datalake.spark3.transformation.{Custom, Transformation}
import bio.ferlab.fhir.etl.transformations.Transformations.extractionMappings
import org.apache.spark.sql.functions.{col, regexp_extract}
import play.api.libs.json.{JsValue, Json}

import scala.util.matching.Regex

object FhavroToNormalizedMappings {
  val pattern: Regex = "raw_([A-Za-z0-9]+)".r

  val idFromUrlRegex = "https://kf-api-fhir-service.kidsfirstdrc.org/[A-Za-z]+/([0-9A-Za-z]+)/_history/2"



  val INGESTION_TIMESTAMP = "ingested_on"

  val defaultTransformations: List[Transformation] = List(
    Custom(_
      .withColumn("fhir_id", regexp_extract(col("id"), idFromUrlRegex, 1))
    )
  )


  val source: String = scala.io.Source.fromFile("src/main/scala/bio/ferlab/fhir/etl/fhavro/kfdrc-documentreference.avsc").mkString
  val asJson: JsValue = Json.parse(source)

  def mappings(implicit c: Configuration): List[(DatasetConf, DatasetConf, List[Transformation])] = c.sources.filter(s => s.format == Format.AVRO).map(s =>
    {
      val pattern(table ) = s.id
      (s.copy(readoptions = Map("avroSchema" -> asJson.toString())), c.getDataset(s"normalized_$table"), defaultTransformations ++ extractionMappings(table))
    }
  )
}
