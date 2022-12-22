package bio.ferlab.fhir.etl

import bio.ferlab.datalake.commons.config.{ConfigurationWrapper, DatalakeConf}
import bio.ferlab.datalake.spark3.{SparkApp, SparkAppWithConfig}
import bio.ferlab.fhir.etl.config.ETLConfiguration
import bio.ferlab.fhir.etl.fhavro.FhavroToNormalizedMappings
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.generic.auto._
import pureconfig.module.enum._

object ImportTask extends SparkAppWithConfig[ETLConfiguration] {
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  LOGGER.info(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(_, _, releaseId, studyIds) = args

  val studyList = studyIds.split(",").toList

  implicit val (conf, _, spark) = init()

  val jobs = FhavroToNormalizedMappings
    .mappings(releaseId)
    .map { case (src, dst, transformations) => new ImportRawToNormalizedETL(src, dst, transformations, releaseId, studyList) }

    jobs.foreach(_.run())
}

