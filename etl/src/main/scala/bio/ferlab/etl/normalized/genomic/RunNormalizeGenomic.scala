package bio.ferlab.etl.normalized.genomic

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{ParserForMethods, arg, main}

object RunNormalizeGenomic {
  @main
  def snv(rc: RuntimeETLContext,
          @arg(name = "study-id", short = 's', doc = "Study Id") studyId: String,
          @arg(name = "release-id", short = 'r', doc = "Release Id") releaseId: String,
          @arg(name = "vcf-pattern", short = 'v', doc = "VCF Pattern") vcfPattern: String,
          @arg(name = "reference-genome-path", short = 'g', doc = "Reference Genome Path") referenceGenomePath: Option[String]): Unit = SNV(rc, studyId, releaseId, vcfPattern, referenceGenomePath).run()


  @main
  def consequences(rc: RuntimeETLContext,
          @arg(name = "study-id", short = 's', doc = "Study Id") studyId: String,
          @arg(name = "vcf-pattern", short = 'v', doc = "VCF Pattern") vcfPattern: String,
          @arg(name = "reference-genome-path", short = 'g', doc = "Reference Genome Path") referenceGenomePath: Option[String]): Unit = Consequences(rc, studyId, vcfPattern, referenceGenomePath).run()

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)


}