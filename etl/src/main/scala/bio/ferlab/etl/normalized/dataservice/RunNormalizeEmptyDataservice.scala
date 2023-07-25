package bio.ferlab.etl.normalized.dataservice

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{ParserForMethods, main}

/**
 * Use for initializing empty delta table, for project that does not use dataservice
 */
object RunNormalizeEmptyDataservice {

  @main
  def run(rc: RuntimeETLContext): Unit = EmptyDataserviceExportETL(rc).run()


  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)

}
