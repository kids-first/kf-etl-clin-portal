package bio.ferlab.fhir.etl.config

import bio.ferlab.datalake.commons.config.Format.{AVRO, PARQUET}
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.S3

object ConfigurationGenerator extends App {
  //TODO check is args is empty

  val studies = args.slice(1, args.length)
  val release = args(0)

  val input = "kfdrc"
  val output = "output"

  val database = "kfdrc"

  val storage = List(
    StorageConf(input, "s3a://kfdrc", S3),
    StorageConf(output, "s3a://output", S3)
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

//  val sourceNames = Seq("condition", "documentreference", "group", "observation", "patient", "researchstudy", "researchsubject", "specimen")
//val sourceNames = Seq("condition", "group", "observation", "patient", "researchstudy", "researchsubject", "specimen")
val sourceNames = Seq("condition")

  val sources = sourceNames.flatMap(sn =>
    studies.flatMap(study => {
      Seq(
        DatasetConf(s"raw_$sn", input, s"/raw/fhir/$sn/study=$study", AVRO, OverWrite),
        DatasetConf(
          s"normalized_$sn",
          output,
          s"/normalized/fhir/$sn/study=$study/release=$release",
          PARQUET,
          OverWrite,
          TableConf("kfdrc", s"fhir_$sn")
        )
      )
    })
  ).toList


  val local_conf = Configuration(
    storages = storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(database, t.name)))),
    args=args.toList,
    sparkconf = local_spark_conf
  )

  ConfigurationWriter.writeTo("./src/main/resources/config/dev.conf", local_conf)
}
