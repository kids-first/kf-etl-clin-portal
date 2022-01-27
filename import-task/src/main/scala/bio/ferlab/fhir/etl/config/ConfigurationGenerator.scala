package bio.ferlab.fhir.etl.config

import bio.ferlab.datalake.commons.config.Format.{AVRO, PARQUET}
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.S3

object ConfigurationGenerator extends App {
  val raw = "raw"
  val normalized = "normalized"

  val database = "normalized"

  val storage = List(
    StorageConf(raw, "s3a://raw", S3),
    StorageConf(normalized, "s3a://normalized", S3)
  )

  val local_spark_conf = Map(
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
    "spark.delta.merge.repartitionBeforeWrite" -> "true",
    "spark.sql.legacy.timeParserPolicy" -> "CORRECTED",
    "spark.sql.mapKeyDedupPolicy" -> "LAST_WIN",
    "spark.master" -> "local",
    "spark.hadoop.fs.s3a.access.key" -> "minioadmin",
    "spark.hadoop.fs.s3a.secret.key" -> "minioadmin",
    "spark.hadoop.fs.s3a.endpoint" -> "http://127.0.0.1:9000",
    "spark.hadoop.fs.s3a.path.style.access" -> "true",
    "spark.hadoop.fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem"
  )

  val sourceNames: Seq[(String, Option[String], List[String])] = Seq(
    ("observation", Some("family-relationship"), List("study_id", "release_id")),
    ("observation", Some("vital-status"), List("study_id", "release_id")),
    ("condition", Some("disease"), List("study_id", "release_id")),
    ("condition", Some("phenotype"), List("study_id", "release_id")),
    ("patient", None, List("study_id", "release_id")),
    ("group", None, List("study_id", "release_id")),
    ("documentreference", None, List("study_id", "release_id")),
    ("researchstudy", None, List("study_id", "release_id")),
    ("researchsubject", None, List("study_id", "release_id")),
    ("specimen", None, List("study_id", "release_id")),
    ("organization", None, List("release_id")),
  )

  val sources = sourceNames.flatMap(sn => {
    val (profileDash, profileUnderscore) = sn._2 match {
      case Some(p) => (s"/$p", s"_$p")
      case None => ("", "")
    }
    Seq(
      DatasetConf(
        id = s"raw_${sn._1}$profileUnderscore",
        storageid = raw,
        path = s"/fhir/${sn._1}$profileDash",
        format = AVRO,
        loadtype = OverWrite,
        partitionby = sn._3
      ),
      DatasetConf(
        id = s"normalized_${sn._1}$profileUnderscore",
        storageid = normalized,
        path = s"/fhir/${sn._1}$profileDash",
        format = PARQUET,
        loadtype = OverWrite,
        table = Some(TableConf(database, s"fhir_${sn._1}")),
        partitionby = sn._3
      )
    )
  }).toList

  val local_conf = Configuration(
    storages = storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(database, t.name)))),
    args = args.toList,
    sparkconf = local_spark_conf
  )

  ConfigurationWriter.writeTo("src/main/resources/config/dev.conf", local_conf)
}
