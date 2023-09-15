package bio.ferlab.etl.normalized.genomic

import bio.ferlab.fhir.etl.config.KFRuntimeETLContext
import mainargs.{ParserForMethods, arg, main}

object RunNormalizeGenomic {
  @main
  def snv(rc: KFRuntimeETLContext,
          @arg(name = "study-id", short = 's', doc = "Study Id") studyId: String,
          @arg(name = "release-id", short = 'r', doc = "Release Id") releaseId: String,
          @arg(name = "reference-genome-path", short = 'g', doc = "Reference Genome Path") referenceGenomePath: Option[String]): Unit = SNV(rc, studyId, releaseId, referenceGenomePath).run()


  @main
  def consequences(rc: KFRuntimeETLContext,
                   @arg(name = "study-id", short = 's', doc = "Study Id") studyId: String,

                   @arg(name = "reference-genome-path", short = 'g', doc = "Reference Genome Path") referenceGenomePath: Option[String]): Unit = Consequences(rc, studyId, referenceGenomePath).run()

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)


}