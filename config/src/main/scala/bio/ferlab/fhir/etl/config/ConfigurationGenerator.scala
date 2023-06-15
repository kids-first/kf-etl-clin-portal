package bio.ferlab.fhir.etl.config

import bio.ferlab.datalake.commons.config.Format.{AVRO, DELTA, JSON, PARQUET}
import bio.ferlab.datalake.commons.config.LoadType.{OverWrite, OverWritePartition, Read, Scd1}
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.S3
import pureconfig.generic.auto._

case class SourceConfig(entityType: String, partitionBy: List[String])

case class Index(name: String, partitionBy: List[String])


object ConfigurationGenerator extends App {
  val pInclude = "include"
  val pKfStrides = "kf-strides"

  private def populateTable(sources: List[DatasetConf], database: String): List[DatasetConf] = {
    sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(database, t.name))))
  }

  def isFlatSpecimenModel(project: String): Boolean = project == pKfStrides

  private val partitionByStudyId = List("study_id")
  val sourceNames: Seq[SourceConfig] = Seq(
    SourceConfig("family_relationship", partitionByStudyId),
    SourceConfig("vital_status", partitionByStudyId),
    SourceConfig("histology_observation", partitionByStudyId),
    SourceConfig("disease", partitionByStudyId),
    SourceConfig("proband_observation", partitionByStudyId),
    SourceConfig("phenotype", partitionByStudyId),
    SourceConfig("patient", partitionByStudyId),
    SourceConfig("group", partitionByStudyId),
    SourceConfig("document_reference", partitionByStudyId),
    SourceConfig("research_study", partitionByStudyId),
    SourceConfig("research_subject", partitionByStudyId),
    SourceConfig("specimen", partitionByStudyId),
    SourceConfig("organization", Nil)
  )

  val storage = "storage"

  val rawsAndNormalized = sourceNames.flatMap(source => {
    val rawPath = source.entityType
    val tableName = source.entityType.replace("-", "_")
    Seq(
      DatasetConf(
        id = s"raw_$tableName",
        storageid = storage,
        path = s"/fhir/$rawPath",
        format = AVRO,
        loadtype = Read,
        partitionby = source.partitionBy :+ "release_id"
      ),
      DatasetConf(
        id = s"normalized_$tableName",
        storageid = storage,
        path = s"/normalized/$rawPath",
        format = DELTA,
        loadtype = if (source.partitionBy.nonEmpty) OverWritePartition else OverWrite,
        table = Some(TableConf("database", tableName)),
        partitionby = source.partitionBy,
        writeoptions = WriteOptions.DEFAULT_OPTIONS ++ Map("overwriteSchema" -> "true"),
        repartition = Some(Coalesce(10))
      )
    )
  })

  val dataserviceDatasets = Seq("sequencing_experiment", "sequencing_experiment_genomic_file", "sequencing_center").map { entity =>
    DatasetConf(
      id = s"normalized_$entity",
      storageid = storage,
      path = s"/normalized/$entity",
      format = DELTA,
      loadtype = OverWritePartition,
      table = Some(TableConf("database", entity)),
      partitionby = partitionByStudyId,
      writeoptions = WriteOptions.DEFAULT_OPTIONS ++ Map("overwriteSchema" -> "true")
    )

  }

  val sources = (rawsAndNormalized ++ dataserviceDatasets ++ Seq(
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
  ) ++ Seq(
    DatasetConf(
      id = "simple_participant",
      storageid = storage,
      path = s"/es_index/fhir/simple_participant",
      format = PARQUET,
      loadtype = OverWrite,
      partitionby = partitionByStudyId,
      repartition = Some(Coalesce(20))

    )
  ) ++ Seq(
    DatasetConf(
      id = "enriched_histology_disease",
      storageid = storage,
      path = s"/enriched/histology_disease",
      format = DELTA,
      loadtype = OverWritePartition,
      table = Some(TableConf("database", "histology_disease")),
      partitionby = partitionByStudyId,
      writeoptions = WriteOptions.DEFAULT_OPTIONS ++ Map("overwriteSchema" -> "true")
    ),
    DatasetConf(
      id = "enriched_specimen",
      storageid = storage,
      path = s"/enriched/specimen",
      format = DELTA,
      loadtype = OverWritePartition,
      table = Some(TableConf("database", "enriched_specimen")),
      partitionby = partitionByStudyId,
      writeoptions = WriteOptions.DEFAULT_OPTIONS ++ Map("overwriteSchema" -> "true")
    )
  ) ++ Seq(
    DatasetConf(
      id = "normalized_snv",
      storageid = storage,
      path = s"/normalized/snv",
      format = DELTA,
      loadtype = OverWritePartition,
      table = Some(TableConf("database", "normalized_snv")),
      partitionby = List("study_id", "chromosome"),
      writeoptions = WriteOptions.DEFAULT_OPTIONS ++ Map("overwriteSchema" -> "true"),
      repartition = Some(RepartitionByColumns(Seq("chromosome"), Some(100)))
    ),
    DatasetConf(
      id = "normalized_consequences",
      storageid = storage,
      path = "/normalized/consequences",
      format = DELTA,
      loadtype = Scd1,
      partitionby = List("chromosome"),
      table = Some(TableConf("database", "normalized_consequences")),
      keys = List("chromosome", "start", "reference", "alternate", "ensembl_transcript_id"),
      repartition = Some(RepartitionByColumns(Seq("chromosome"), Some(100)))
    ),
    DatasetConf(
      id = "enriched_specimen",
      storageid = storage,
      path = s"/enriched/specimen",
      format = DELTA,
      loadtype = OverWritePartition,
      table = Some(TableConf("database", "enriched_specimen")),
      partitionby = partitionByStudyId,
      writeoptions = WriteOptions.DEFAULT_OPTIONS ++ Map("overwriteSchema" -> "true")
    )
  ) ++ Seq(
    Index("study_centric", partitionByStudyId),
    Index("participant_centric", partitionByStudyId),
    Index("file_centric", partitionByStudyId),
    Index("biospecimen_centric", partitionByStudyId),
  ).flatMap(index => {
    Seq(
      DatasetConf(
        id = s"es_index_${index.name}",
        storageid = storage,
        path = s"/es_index/fhir/${index.name}",
        format = PARQUET,
        loadtype = OverWrite,
        table = Some(TableConf("database", s"es_index_${index.name}")),
        partitionby = index.partitionBy,
        repartition = Some(Coalesce(20))
      )
    )
  })).toList

  val includeConf = Map("qaDbName" -> "include_portal_qa", "prdDbName" -> "include_portal_prd", "localDbName" -> "normalized", "bucketNamePrefix" -> "include-373997854230-datalake")
  val kfConf = Map("qaDbName" -> "kf_portal_qa", "prdDbName" -> "kf_portal_prd", "localDbName" -> "normalized", "bucketNamePrefix" -> "kf-strides-232196027141-datalake")
  val conf = Map(pInclude -> includeConf, pKfStrides -> kfConf)
  val spark_conf = Map(
    "spark.databricks.delta.merge.repartitionBeforeWrite.enabled" -> "true",
    "spark.databricks.delta.schema.autoMerge.enabled" -> "true",
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
    "spark.delta.merge.repartitionBeforeWrite" -> "true",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" -> "CORRECTED",
    "spark.sql.legacy.timeParserPolicy" -> "CORRECTED",
    "spark.sql.mapKeyDedupPolicy" -> "LAST_WIN",
    "spark.hadoop.fs.s3a.multiobjectdelete.enable" -> "false", //https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html#MultiObjectDeleteException_during_delete_or_rename_of_files,
    "spark.databricks.delta.replaceWhere.constraintCheck.enabled" -> "false"
  )
  conf.foreach { case (project, _) =>
    ConfigurationWriter.writeTo(s"config/output/config/dev-${project}.conf", ETLConfiguration(isFlatSpecimenModel(project), DatalakeConf(
      storages = List(
        StorageConf(storage, "s3a://storage", S3)
      ),
      sources = populateTable(sources, conf(project)("localDbName")),
      args = args.toList,
      sparkconf = Map(
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
        "spark.databricks.delta.merge.repartitionBeforeWrite.enabled" -> "true",
        "spark.databricks.delta.schema.autoMerge.enabled" -> "true"
      )
    ),
      dataservice_url = "https://kf-api-dataservice-qa.kidsfirstdrc.org"

    ))


    ConfigurationWriter.writeTo(s"config/output/config/qa-${project}.conf", ETLConfiguration(isFlatSpecimenModel(project), DatalakeConf(
      storages = List(
        StorageConf(storage, s"s3a://${conf(project)("bucketNamePrefix")}-qa", S3)
      ),
      sources = populateTable(sources, conf(project)("qaDbName")),
      args = args.toList,
      sparkconf = spark_conf
    ),
      dataservice_url = "https://kf-api-dataservice-qa.kidsfirstdrc.org"
    ))

    ConfigurationWriter.writeTo(s"config/output/config/prd-${project}.conf", ETLConfiguration(isFlatSpecimenModel(project), DatalakeConf(
      storages = List(
        StorageConf(storage, s"s3a://${conf(project)("bucketNamePrefix")}-prd", S3)
      ),
      sources = populateTable(sources, conf(project)("prdDbName")),
      args = args.toList,
      sparkconf = spark_conf
    ),
      dataservice_url = "https://kf-api-dataservice.kidsfirstdrc.org"
    ))
  }

  ConfigurationWriter.writeTo(s"config/output/config/ucsf.conf", ETLConfiguration(isFlatSpecimenModel = true, DatalakeConf(
    storages = List(
      StorageConf(storage, s"s3a://d3b-portal-65-4-r-us-west-2.sec.ucsf.edu", S3)
    ),
    sources = sources.map(ds => ds.copy(table = None)),
    args = args.toList,
    sparkconf = spark_conf
  ),
    dataservice_url = ""
  ))
}
