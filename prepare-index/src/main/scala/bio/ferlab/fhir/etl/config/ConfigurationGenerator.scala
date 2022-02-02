package bio.ferlab.fhir.etl.config

import bio.ferlab.datalake.commons.config.Format.{JSON, PARQUET}
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.S3

object ConfigurationGenerator extends App {
  val esindex = "esindex"
  val normalized = "normalized"
  val hpoterms = "hpoterms"
  val mondoterms = "mondoterms"

  val local_database = "normalized"
  val qa_database = "portal-qa-normalized"
  val prd_database = "portal-prd-normalized"

  val local_storage = List(
    StorageConf(normalized, "s3a://normalized", S3),
    StorageConf(esindex, "s3a://esindex", S3),
    StorageConf(hpoterms, "s3a://hpoterms", S3),
    StorageConf(mondoterms, "s3a://mondoterms", S3),
  )

  val qa_storage = List(
    StorageConf(normalized, "s3a://etl-clin-portal-qa-normalized", S3),
    StorageConf(esindex, "s3a://etl-clin-portal-qa-esindex", S3),
    StorageConf(hpoterms, "s3a://etl-clin-portal-qa-hpoterms", S3),
    StorageConf(mondoterms, "s3a://etl-clin-portal-qa-mondoterms", S3),
  )

  val prd_storage = List(
    StorageConf(normalized, "s3a://etl-clin-portal-prd-normalized", S3),
    StorageConf(esindex, "s3a://etl-clin-portal-prd-esindex", S3),
    StorageConf(hpoterms, "s3a://etl-clin-portal-prd-hpoterms", S3),
    StorageConf(mondoterms, "s3a://etl-clin-portal-prd-mondoterms", S3),
  )

  val local_spark_conf = Map(
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
    "spark.delta.merge.repartitionBeforeWrite" -> "true",
    "spark.hadoop.fs.s3a.access.key" -> "minioadmin",
    "spark.hadoop.fs.s3a.endpoint" -> "http://127.0.0.1:9000",
    "spark.hadoop.fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access" -> "true",
    "spark.hadoop.fs.s3a.secret.key" -> "minioadmin",
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

  val normalizedNames: Seq[(String, Option[String], List[String])] = Seq(
    ("researchstudy", None, List("study_id", "release_id")),
    ("patient", None, List("study_id", "release_id")),
    ("group", None, List("study_id", "release_id")),
    ("documentreference", Some("drs-document-reference"), List("study_id", "release_id")),
    ("specimen", None, List("study_id", "release_id")),
    ("condition", Some("disease"), List("study_id", "release_id")),
    ("condition", Some("phenotype"), List("study_id", "release_id")),
    ("observation", Some("family-relationship"), List("study_id", "release_id")),
    ("observation", Some("vital-status"), List("study_id", "release_id")),
    ("researchsubject", None, List("study_id", "release_id")),
    ("organization", None, List("release_id")),
  )

  val tmpData: Seq[(String, List[String])] = Seq(
    ("simple_participant", List("study_id", "release_id")),
  )

  val indexNames: Seq[(String, List[String])] = Seq(
    ("study_centric", List("study_id", "release_id")),
    ("participant_centric", List("study_id", "release_id")),
    ("file_centric", List("study_id", "release_id")),
    ("biospecimen_centric", List("study_id", "release_id")),
  )

  val normalizeds = normalizedNames.flatMap(sn => {
    val (profileDash, profileUnderscore) = sn._2 match {
      case Some(p) => (s"/$p", s"_$p")
      case None => ("", "")
    }
    Seq(
      DatasetConf(
        id = s"normalized_${sn._1}$profileUnderscore",
        storageid = normalized,
        path = s"/fhir/${sn._1}$profileDash",
        format = PARQUET,
        loadtype = OverWrite,
        table = Some(TableConf("database", s"fhir_${sn._1}")),
        partitionby = sn._3
      )
    )
  }).toList

  val tmpDatas = tmpData.flatMap(sn => {
    Seq(
      DatasetConf(
        id = s"${sn._1}",
        storageid = esindex,
        path = s"/es_index/fhir/${sn._1}",
        format = PARQUET,
        loadtype = OverWrite,
        partitionby = sn._2
      )
    )
  }).toList

  val indexs = indexNames.flatMap(sn => {
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

  val terms = List(
    DatasetConf(
      id = "hpo_terms",
      storageid = hpoterms,
      path = s"/hpo_terms",
      format = JSON,
      loadtype = OverWrite,
    ),
    DatasetConf(
      id = "mondo_terms",
      storageid = mondoterms,
      path = s"/mondo_terms",
      format = JSON,
      loadtype = OverWrite,
    )
  )

  val sources = normalizeds ++ terms ++ tmpDatas ++ indexs

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

  ConfigurationWriter.writeTo("src/main/resources/config/dev.conf", local_conf)
  ConfigurationWriter.writeTo("src/main/resources/config/qa.conf", qa_conf)
  ConfigurationWriter.writeTo("src/main/resources/config/prd.conf", prd_conf)
}
