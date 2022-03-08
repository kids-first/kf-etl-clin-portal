package bio.ferlab.fhir.etl.config

import bio.ferlab.datalake.commons.config.Format.{AVRO, DELTA, JSON, PARQUET}
import bio.ferlab.datalake.commons.config.LoadType.{OverWrite, OverWritePartition}
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.S3

object ConfigurationGenerator extends App {

  val storage = "storage"
  val local_database = "normalized"
  val qa_database = "include_portal_qa"
  val prd_database = "include_portal_prd"

  val local_storage = List(
    StorageConf(storage, "s3a://storage", S3)
  )

  val qa_storage = List(
    StorageConf(storage, "s3a://include-373997854230-datalake-qa", S3)
  )

  val prd_storage = List(
    StorageConf(storage, "s3a://include-373997854230-datalake-prd", S3)
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

  private val partitionByStudyIdAndReleaseId = List("study_id", "release_id")
  val sourceNames: Seq[SourceConfig] = Seq(
    SourceConfig("observation", Some("family-relationship"), partitionByStudyIdAndReleaseId),
    SourceConfig("observation", Some("vital-status"), partitionByStudyIdAndReleaseId),
    SourceConfig("condition", Some("disease"), partitionByStudyIdAndReleaseId),
    SourceConfig("condition", Some("phenotype"), partitionByStudyIdAndReleaseId),
    SourceConfig("patient", None, partitionByStudyIdAndReleaseId),
    SourceConfig("group", None, partitionByStudyIdAndReleaseId),
    SourceConfig("documentreference", Some("drs-document-reference"), partitionByStudyIdAndReleaseId),
    SourceConfig("researchstudy", None, partitionByStudyIdAndReleaseId),
    SourceConfig("researchsubject", None, partitionByStudyIdAndReleaseId),
    SourceConfig("specimen", None, partitionByStudyIdAndReleaseId),
    SourceConfig("organization", None, List("release_id")),
    SourceConfig("task", None, partitionByStudyIdAndReleaseId),
  )

  case class SourceConfig(fhirResource: String, fhirProfile: Option[String], partitionBy: List[String])

  case class Index(name: String, partitionBy: List[String])

  val rawsAndNormalized = sourceNames.flatMap(source => {

    val rawPath = source.fhirProfile.map(p => s"${source.fhirResource}/$p").getOrElse(source.fhirResource)
    val tableName = source.fhirProfile.map(_.replace("-", "_")).getOrElse(source.fhirResource.replace("-", "_"))
    Seq(
      DatasetConf(
        id = s"raw_$tableName",
        storageid = storage,
        path = s"/fhir/$rawPath",
        format = AVRO,
        loadtype = OverWrite,
        table = Some(TableConf("database", s"raw_$tableName")),
        partitionby = source.partitionBy
      ),
      DatasetConf(
        id = s"normalized_$tableName",
        storageid = storage,
        path = s"/normalized/$rawPath",
        format = DELTA,
        loadtype = OverWritePartition,
        table = Some(TableConf("database", tableName)),
        partitionby = source.partitionBy,
        writeoptions = WriteOptions.DEFAULT_OPTIONS ++ Map("overwriteSchema"-> "true")
      )
    )
  })


  val indexes: Seq[DatasetConf] = Seq(
    Index("study_centric", partitionByStudyIdAndReleaseId),
    Index("participant_centric", partitionByStudyIdAndReleaseId),
    Index("file_centric", partitionByStudyIdAndReleaseId),
    Index("biospecimen_centric", partitionByStudyIdAndReleaseId),
  ).flatMap(index => {
    Seq(
      DatasetConf(
        id = s"es_index_${index.name}",
        storageid = storage,
        path = s"/es_index/fhir/${index.name}",
        format = PARQUET,
        loadtype = OverWrite,
        table = Some(TableConf("database", s"es_index_${index.name}")),
        partitionby = index.partitionBy
      )
    )
  })

  val terms = Seq(
    DatasetConf(
      id = "hpo_terms",
      storageid = storage,
      path = s"/hpo_terms",
      format = JSON,
      table = Some(TableConf("database", "hpo_terms")),
      loadtype = OverWrite,
    ),
    DatasetConf(
      id = "mondo_terms",
      storageid = storage,
      path = s"/mondo_terms",
      table = Some(TableConf("database", "mondo_terms")),
      format = JSON,
      loadtype = OverWrite,
    )
  )

  val tmpDatas = Seq(
    DatasetConf(
      id = "simple_participant",
      storageid = storage,
      path = s"/es_index/fhir/simple_participant",
      format = PARQUET,
      loadtype = OverWrite,
      partitionby = partitionByStudyIdAndReleaseId
    )
  )

  val sources = (rawsAndNormalized ++ terms ++ tmpDatas ++ indexes).toList

  val local_conf = Configuration(
    storages = local_storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(local_database, t.name)))),
    args = args.toList,
    sparkconf = local_spark_conf
  )

  val qa_conf = Configuration(
    storages = qa_storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(qa_database, t.name)))),
    args = args.toList,
    sparkconf = spark_conf
  )

  val prd_conf = Configuration(
    storages = prd_storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(prd_database, t.name)))),
    args = args.toList,
    sparkconf = spark_conf
  )

  ConfigurationWriter.writeTo("config/output/config/dev.conf", local_conf)
  ConfigurationWriter.writeTo("config/output/config/qa.conf", qa_conf)
  ConfigurationWriter.writeTo("config/output/config/prd.conf", prd_conf)
}
