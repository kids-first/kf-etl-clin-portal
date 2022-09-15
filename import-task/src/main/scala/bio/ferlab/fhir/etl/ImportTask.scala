package bio.ferlab.fhir.etl

import bio.ferlab.datalake.spark3.public.SparkApp
import bio.ferlab.fhir.etl.Utils.allowedProjects
import bio.ferlab.fhir.etl.fhavro.FhavroToNormalizedMappings
import org.slf4j.{Logger, LoggerFactory}

object ImportTask extends SparkApp {
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  LOGGER.info(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(_, _, releaseId, studyIds, project) = args

  if (!allowedProjects.contains(project)) {
    LOGGER.error(s"project value must be one of: ${allowedProjects.mkString(",")}. Got: $project")
    System.exit(-1)
  }

  val studyList = studyIds.split(",").toList

  implicit val (conf, _, spark) = init()

  val jobs = FhavroToNormalizedMappings
    .mappings(releaseId, project)
    .map { case (src, dst, transformations) => new ImportRawToNormalizedETL(src, dst, transformations, releaseId, studyList) }

    jobs.foreach(_.run())
}
