package bio.ferlab.etl.normalized.clinical

import bio.ferlab.datalake.spark3.SparkAppWithConfig
import bio.ferlab.fhir.etl.config.ETLConfiguration
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.generic.auto._

object RunNormalizeClinical extends SparkAppWithConfig[ETLConfiguration] {
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  LOGGER.info(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(_, _, releaseId, studyIds) = args

  val studyList = studyIds.split(",").toList

  implicit val (conf, _, spark) = init()

  val jobs = FhirToNormalizedMappings
    .mappings(releaseId)
    .map { case (src, dst, transformations) => new NormalizeClinicalETL(src, dst, transformations, releaseId, studyList) }

  jobs.foreach(_.run())
}
