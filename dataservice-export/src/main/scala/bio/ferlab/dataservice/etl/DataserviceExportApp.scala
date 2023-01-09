package bio.ferlab.dataservice.etl

import bio.ferlab.datalake.spark3.SparkAppWithConfig
import bio.ferlab.fhir.etl.config.ETLConfiguration
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.generic.auto._
import pureconfig.module.enum._

object DataserviceExportApp extends SparkAppWithConfig[ETLConfiguration] {
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  LOGGER.info(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(_, _, releaseId, studyIds) = args

  val studyList = studyIds.split(",").toList

  implicit val (conf, _, spark) = init()
  DefaultContext.withContext { c =>
    import c.implicits._
    import scala.concurrent.ExecutionContext.Implicits._
    val retriever = EntityDataRetriever(conf.dataservice_url, Seq("visible=true"))
    new DataserviceExportETL(releaseId, studyList, retriever).run()
  }

}

