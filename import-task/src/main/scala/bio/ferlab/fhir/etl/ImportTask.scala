package bio.ferlab.fhir.etl

import bio.ferlab.datalake.spark3.etl.{ETL, RawToNormalizedETL}
import bio.ferlab.datalake.spark3.public.SparkApp
import bio.ferlab.fhir.etl.fhavro.FhavroToNormalizedMappings

object ImportTask extends SparkApp {

  implicit val (conf, spark) = init()
  spark.sparkContext.setLogLevel("WARN")

  val jobs: List[ETL] =
    FhavroToNormalizedMappings
      .mappings
      .map { case (src, dst, transformations) => new RawToNormalizedETL(src, dst, transformations) }

//  spark.read.format("avro").options(avroSchema=jsonFormatSchema).load("/tmp/avro")

    jobs.foreach(_.run().printSchema())
  jobs.foreach(_.run().show(20, false))
}
