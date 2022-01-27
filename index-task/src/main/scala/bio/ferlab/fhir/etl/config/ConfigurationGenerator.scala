package bio.ferlab.fhir.etl.config

import bio.ferlab.datalake.commons.config.Format.{JSON, PARQUET}
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.S3

object ConfigurationGenerator extends App {
  val esindex = "esindex"

  val local_storage = List(
    StorageConf(esindex, "s3a://esindex", S3),
  )

  val qa_storage = List(
    StorageConf(esindex, "s3a://etl-clin-portal-qa-esindex", S3),
  )

  val prd_storage = List(
    StorageConf(esindex, "s3a://etl-clin-portal-prd-esindex", S3),
  )

  val local_spark_conf = Map(
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
    "spark.delta.merge.repartitionBeforeWrite" -> "true",
    "spark.hadoop.fs.s3a.access.key" -> "${?AWS_ACCESS_KEY}",
    "spark.hadoop.fs.s3a.endpoint" -> "${?AWS_ENDPOINT}",
    "spark.hadoop.fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access" -> "true",
    "spark.hadoop.fs.s3a.secret.key" -> "${?AWS_SECRET_KEY}",
    "spark.master" -> "local",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.legacy.timeParserPolicy" -> "CORRECTED",
    "spark.sql.mapKeyDedupPolicy" -> "LAST_WIN",
  )

  val spark_conf = Map(
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
    "spark.delta.merge.repartitionBeforeWrite" -> "true",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" -> "CORRECTED",
    "spark.sql.legacy.timeParserPolicy" -> "CORRECTED",
    "spark.sql.mapKeyDedupPolicy" -> "LAST_WIN",
  )

  val indexNames: Seq[(String, List[String])] = Seq(
    ("study_centric", List("study_id", "release_id")),
    ("participant_centric", List("study_id", "release_id")),
    ("file_centric", List("study_id", "release_id")),
    ("biospecimen_centric", List("study_id", "release_id")),
  )

  val sources = indexNames.flatMap(sn => {
    Seq(
      DatasetConf(
        id = s"es_index_${sn._1}",
        storageid = esindex,
        path = s"/es_index/fhir/${sn._1}",
        format = PARQUET,
        loadtype = OverWrite,
        partitionby = sn._2
      )
    )
  }).toList

  val local_conf = Configuration(
    storages = local_storage,
    sources = sources,
    args = args.toList,
    sparkconf = local_spark_conf
  )

  val qa_conf = Configuration(
    storages = qa_storage,
    sources = sources,
    args = args.toList,
    sparkconf = spark_conf
  )

  val prd_conf = Configuration(
    storages = prd_storage,
    sources = sources,
    args = args.toList,
    sparkconf = spark_conf
  )

  ConfigurationWriter.writeTo("src/main/resources/config/dev.conf", local_conf)
  ConfigurationWriter.writeTo("src/main/resources/config/qa.conf", qa_conf)
  ConfigurationWriter.writeTo("src/main/resources/config/prd.conf", prd_conf)
}
