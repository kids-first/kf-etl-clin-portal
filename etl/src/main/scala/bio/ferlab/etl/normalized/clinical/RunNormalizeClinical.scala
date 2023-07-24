package bio.ferlab.etl.normalized.clinical

import bio.ferlab.fhir.etl.config.KFRuntimeETLContext
import mainargs.{ParserForMethods, arg}

object RunNormalizeClinical {

  def run(rc: KFRuntimeETLContext,
          @arg(name = "study-id", short = 's', doc = "Study Id") studyIds: List[String],
          @arg(name = "release-id", short = 'r', doc = "Release Id") releaseId: String): Unit = {
    val jobs = FhirToNormalizedMappings
      .mappings(releaseId, rc.config)
      .map { case (src, dst, transformations) => new NormalizeClinicalETL(rc, src, dst, transformations, releaseId, studyIds) }
    jobs.foreach(_.run())
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)

}
