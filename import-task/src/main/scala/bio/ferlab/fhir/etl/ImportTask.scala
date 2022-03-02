package bio.ferlab.fhir.etl

import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.public.SparkApp
import bio.ferlab.fhir.etl.fhavro.FhavroToNormalizedMappings

object ImportTask extends SparkApp {

  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(_, _, releaseId, studyIds) = args

  val studyList = studyIds.split(";").toList

  implicit val (conf, _, spark) = init()

  val jobs = FhavroToNormalizedMappings
    .mappings(releaseId)
    .map { case (src, dst, transformations) => new ImportRawToNormalizedETL(src, dst, transformations, releaseId, studyList) }

    jobs.foreach(_.run())
}
