package bio.ferlab.fhir.etl

import bio.ferlab.datalake.commons.config.Format.{AVRO, DELTA}
import bio.ferlab.datalake.commons.config.LoadType.{OverWrite, OverWritePartition}
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, StorageConf, TableConf}
import bio.ferlab.datalake.commons.file.FileSystemType.S3
import bio.ferlab.fhir.etl.Utils.{pInclude, pKfStrides}
import bio.ferlab.fhir.etl.fhavro.FhavroToNormalizedMappings.{generateFhirIdColumValueFromIdColum, mappings}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy.CORRECTED
import org.scalatest.{FlatSpec, Matchers}

class FhavroToNormalizedMappingsSpec
  extends FlatSpec
    with Matchers
    with WithSparkSession {

  import spark.implicits._

  val storage = "storage"

  "method generateFhirIdColumFromIdColum" should "extract fhir ids when specific urls" in {
    var df = Seq(
      "http://localhost:8000/Condition/1/_history",
      "https://fhir/a/2/_history",
      "https://fhir/a/b/c/2a/_history",
      "https://fhir/123/xyz"
    ).toDF("id")

    df = df.withColumn("fhir_id", generateFhirIdColumValueFromIdColum())
    df.select(col("fhir_id"))
      .as[String]
      .collect() should contain theSameElementsAs
      Seq("1", "2", "2a", "")
  }

  "method mappings" should "not throw an exception when configs are well-formed (project=kf-strides)" in {
    val c = Configuration(
      List(
        StorageConf(filesystem = S3, id = "storage", path = "s3a://kf-strides")
      ),
      List(
        DatasetConf(
          format = AVRO,
          id = "raw_specimen",
          keys = List.empty,
          loadtype = OverWrite,
          path = "/fhir/specimen",
          storageid = storage,
          table =
            Some(TableConf(database = "kf_portal_qa", name = "raw_specimen"))
        ),
        DatasetConf(
          format = DELTA,
          id = "normalized_specimen",
          keys = List.empty,
          loadtype = OverWritePartition,
          path = "/normalized/specimen",
          storageid = storage,
          table = Some(TableConf(database = "kf_portal_qa", name = "specimen"))
        )
      ),
      List.empty,
      Map(
        "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
        "spark.delta.merge.repartitionBeforeWrite" -> "true",
        "spark.fhir.server.url" -> "https://kf-api-fhir-service-qa.kidsfirstdrc.org",
        "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" -> CORRECTED.toString,
        "spark.sql.legacy.timeParserPolicy" -> CORRECTED.toString,
        "spark.sql.mapKeyDedupPolicy" -> "LAST_WIN,"
      )
    )
    noException should be thrownBy mappings("re", pKfStrides)(c)
  }

  "method mappings" should "not throw an exception when configs are well-formed (project=include)" in {
    val c = Configuration(
      List(
        StorageConf(filesystem = S3, id = "storage", path = "s3a://include")
      ),
      List(
        DatasetConf(
          format = AVRO,
          id = "raw_specimen",
          keys = List.empty,
          loadtype = OverWrite,
          path = "/fhir/specimen",
          storageid = storage,
          table =
            Some(TableConf(database = "include_portal_qa", name = "raw_specimen"))
        ),
        DatasetConf(
          format = DELTA,
          id = "normalized_specimen",
          keys = List.empty,
          loadtype = OverWritePartition,
          path = "/normalized/specimen",
          storageid = storage,
          table = Some(TableConf(database = "include_portal_qa", name = "specimen"))
        )
      ),
      List.empty,
      Map(
        "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
        "spark.delta.merge.repartitionBeforeWrite" -> "true",
        "spark.fhir.server.url" -> "https://include-api-fhir-service-dev.includedcc.org",
        "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" -> CORRECTED.toString,
        "spark.sql.legacy.timeParserPolicy" -> CORRECTED.toString,
        "spark.sql.mapKeyDedupPolicy" -> "LAST_WIN"
      ))
    noException should be thrownBy mappings("re", pInclude)(c)
  }
}
