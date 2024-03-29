package bio.ferlab.etl.normalized.clinical

import bio.ferlab.datalake.commons.config.Format.{AVRO, DELTA}
import bio.ferlab.datalake.commons.config.LoadType.{OverWrite, OverWritePartition}
import bio.ferlab.datalake.commons.config.{DatalakeConf, DatasetConf, StorageConf, TableConf}
import bio.ferlab.datalake.commons.file.FileSystemType.S3
import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.etl.normalized.clinical.FhirToNormalizedMappings.{generateFhirIdColumValueFromIdColum, mappings}
import bio.ferlab.fhir.etl.config.ETLConfiguration
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy.CORRECTED
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FhirToNormalizedMappingsSpec
  extends AnyFlatSpec
    with Matchers
    with WithSparkSession {

  import spark.implicits._

  val storage = "storage"

  "method generateFhirIdColumFromIdColum" should "extract fhir ids when specific urls" in {
    var df = Seq(
      "http://localhost:8000/Condition/1/_history?count=2",
      "https://fhir/a/2/_history/2",
      "https://fhir/a/b/c/2a/_history/4",
      "https://fhir/123/xyz",
      "http://localhost:8000/Patient/433003/_history/2"
    ).toDF("id")

    df = df.withColumn("fhir_id", generateFhirIdColumValueFromIdColum())
    df.select(col("fhir_id"))
      .as[String]
      .collect() should contain theSameElementsAs
      Seq("1", "2", "2a", "", "433003")
  }

  "method mappings" should "not throw an exception when configs are well-formed (focus on specimen)" in {
    val storages = List(
      StorageConf(filesystem = S3, id = "storage", path = "s3a://kf-strides")
    )
    val sources = List(
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
    )
    val sparkConfWithFlatSpecimenEntry = Map(
      "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
      "spark.delta.merge.repartitionBeforeWrite" -> "true",
      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
      "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
      "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" -> CORRECTED.toString,
      "spark.sql.legacy.timeParserPolicy" -> CORRECTED.toString,
      "spark.sql.mapKeyDedupPolicy" -> "LAST_WIN,"
    )
    val c1 = ETLConfiguration(
      dataservice_url = "",
      datalake = DatalakeConf(
        storages,
        sources,
        List.empty,
        sparkConfWithFlatSpecimenEntry
      )
    )
    noException should be thrownBy mappings("re", c1)

    val sparkConfWithoutFlatSpecimenEntry = Map(
      "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
      "spark.delta.merge.repartitionBeforeWrite" -> "true",
      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
      "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
      "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" -> CORRECTED.toString,
      "spark.sql.legacy.timeParserPolicy" -> CORRECTED.toString,
      "spark.sql.mapKeyDedupPolicy" -> "LAST_WIN,"
    )
    val c2 = ETLConfiguration(
      dataservice_url = "",
      datalake = DatalakeConf(
        storages,
        sources,
        List.empty,
        sparkConfWithoutFlatSpecimenEntry
      ))
    noException should be thrownBy mappings("re", c2)
  }
}
