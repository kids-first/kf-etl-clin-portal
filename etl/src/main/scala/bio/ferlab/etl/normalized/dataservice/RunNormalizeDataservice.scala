package bio.ferlab.etl.normalized.dataservice

import bio.ferlab.fhir.etl.config.KFRuntimeETLContext
import mainargs.{ParserForMethods, arg}


object RunNormalizeDataservice {

  def run(rc: KFRuntimeETLContext,
          @arg(name = "study-id", short = 's', doc = "Study Id") studyIds: List[String],
          @arg(name = "release-id", short = 'r', doc = "Release Id") releaseId: String): Unit = DefaultContext.withContext { c =>
    import c.implicits._

    import scala.concurrent.ExecutionContext.Implicits._
    val retriever = EntityDataRetriever(rc.config.dataservice_url, Seq("visible=true"))
    new DataserviceExportETL(rc, releaseId, studyIds, retriever).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)

}
