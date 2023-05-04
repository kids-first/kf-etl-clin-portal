package bio.ferlab.dataservice.etl

import bio.ferlab.datalake.spark3.SparkAppWithConfig
import bio.ferlab.fhir.etl.config.ETLConfiguration
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.generic.auto._

/**
 * Use for initializing empty delta table, for project that does not use dataservice
 */
object EmptyDataserviceExportApp extends SparkAppWithConfig[ETLConfiguration] {
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  LOGGER.info(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(_, _) = args

  implicit val (conf, _, spark) = init()

  new EmptyDataserviceExportETL().run()


}

