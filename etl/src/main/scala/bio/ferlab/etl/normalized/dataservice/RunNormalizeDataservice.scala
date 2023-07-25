package bio.ferlab.etl.normalized.dataservice


import bio.ferlab.etl.mainutils.Studies
import bio.ferlab.fhir.etl.config.KFRuntimeETLContext
import mainargs.{ParserForMethods, arg, main}


object RunNormalizeDataservice {

  @main
  def run(rc: KFRuntimeETLContext,
          studies: Studies,
          @arg(name = "release-id", short = 'r', doc = "Release Id") releaseId: String): Unit = DefaultContext.withContext { c =>
    import c.implicits._

    import scala.concurrent.ExecutionContext.Implicits._
    val retriever = EntityDataRetriever(rc.config.dataservice_url, Seq("visible=true"))
    new DataserviceExportETL(rc, releaseId, studies.ids, retriever).run()
  }


  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)

}
