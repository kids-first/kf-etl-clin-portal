package bio.ferlab.fhir.etl.config

import bio.ferlab.datalake.commons.config.Format.{AVRO, DELTA, JSON, PARQUET}
import bio.ferlab.datalake.commons.config.LoadType.{OverWrite, OverWritePartition}
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.S3

case class SourceConfig(fhirResource: String, entityType: Option[String], partitionBy: List[String])

case class Index(name: String, partitionBy: List[String])


object ConfigurationGenerator extends App {
  val pInclude = "include"
  val pKfStrides = "kf-strides"

  def populateTable(sources: List[DatasetConf], database: String): List[DatasetConf] = {
    sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(database, t.name))))
  }

  def isFlatSpecimenModel(project: String): Boolean = project == pKfStrides

  private val partitionByStudyIdAndReleaseId = List("study_id", "release_id")
  val sourceNames: Seq[SourceConfig] = Seq(
    SourceConfig("observation", Some("family_relationship"), partitionByStudyIdAndReleaseId),
    SourceConfig("observation", Some("vital_status"), partitionByStudyIdAndReleaseId),
    SourceConfig("observation", Some("histology_observation"), partitionByStudyIdAndReleaseId),
    SourceConfig("condition", Some("disease"), partitionByStudyIdAndReleaseId),
    SourceConfig("observation", Some("proband_observation"), partitionByStudyIdAndReleaseId),
    SourceConfig("condition", Some("phenotype"), partitionByStudyIdAndReleaseId),
    SourceConfig("patient", None, partitionByStudyIdAndReleaseId),
    SourceConfig("group", None, partitionByStudyIdAndReleaseId),
    SourceConfig("documentreference", Some("document_reference"), partitionByStudyIdAndReleaseId),
    SourceConfig("researchstudy", Some("research_study"), partitionByStudyIdAndReleaseId),
    SourceConfig("researchsubject", Some("research_subject"), partitionByStudyIdAndReleaseId),
    SourceConfig("specimen", None, partitionByStudyIdAndReleaseId),
    SourceConfig("organization", None, List("release_id"))
  )

  val storage = "storage"

  val rawsAndNormalized = sourceNames.flatMap(source => {
    val rawPath = source.entityType.getOrElse(source.fhirResource)
    val tableName = source.entityType.map(_.replace("-", "_")).getOrElse(source.fhirResource.replace("-", "_"))
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
        writeoptions = WriteOptions.DEFAULT_OPTIONS ++ Map("overwriteSchema" -> "true")
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
      partitionby = partitionByStudyIdAndReleaseId,
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
      partitionby = partitionByStudyIdAndReleaseId
    )
  )++ Seq(
    DatasetConf(
      id = "enriched_histology_disease",
      storageid = storage,
      path = s"/enriched/histology_disease",
      format = DELTA,
      loadtype = OverWritePartition,
      table = Some(TableConf("database", "histology_disease")),
      partitionby = partitionByStudyIdAndReleaseId,
      writeoptions = WriteOptions.DEFAULT_OPTIONS ++ Map("overwriteSchema" -> "true")
    )
  )++ Seq(
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
  })).toList

  val includeConf = Map("fhirDev" -> "https://include-api-fhir-service-dev.includedcc.org", "fhirQa" -> "https://include-api-fhir-service-dev.includedcc.org", "fhirPrd" -> "https://include-api-fhir-service.includedcc.org", "qaDbName" -> "include_portal_qa", "prdDbName" -> "include_portal_prd", "localDbName" -> "normalized", "bucketNamePrefix" -> "include-373997854230-datalake")
  val kfConf = Map("fhirDev" -> "https://kf-api-fhir-service-qa.kidsfirstdrc.org", "fhirQa" -> "https://kf-api-fhir-service-qa.kidsfirstdrc.org", "fhirPrd" -> "https://kf-api-fhir-service.kidsfirstdrc.org", "qaDbName" -> "kf_portal_qa", "prdDbName" -> "kf_portal_prd", "localDbName" -> "normalized", "bucketNamePrefix" -> "kf-strides-232196027141-datalake")
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
    "spark.hadoop.fs.s3a.multiobjectdelete.enable" -> "false", //https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html#MultiObjectDeleteException_during_delete_or_rename_of_files
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
        "spark.fhir.server.url" -> conf(project)("fhirDev"),
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
      sparkconf = spark_conf.++(Map("spark.fhir.server.url" -> conf(project)("fhirQa")))
    ),
      dataservice_url = "https://kf-api-dataservice-qa.kidsfirstdrc.org"
    ))

    ConfigurationWriter.writeTo(s"config/output/config/prd-${project}.conf", ETLConfiguration(isFlatSpecimenModel(project), DatalakeConf(
      storages = List(
        StorageConf(storage, s"s3a://${conf(project)("bucketNamePrefix")}-prd", S3)
      ),
      sources = populateTable(sources, conf(project)("prdDbName")),
      args = args.toList,
      sparkconf = spark_conf.++(Map("spark.fhir.server.url" -> conf(project)("fhirPrd")))
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
    sparkconf = spark_conf.++(Map("spark.fhir.server.url" -> "http://10.90.172.42:443"))
  ),
    dataservice_url = ""
  ))
}
