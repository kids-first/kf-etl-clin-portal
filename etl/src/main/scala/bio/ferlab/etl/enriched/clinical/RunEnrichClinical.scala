package bio.ferlab.etl.enriched.clinical

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{ParserForMethods, arg, main}

object RunEnrichClinical {

  @main
  def histology(rc: RuntimeETLContext, @arg(name = "study-id", short = 's', doc = "Study Id") studies: List[String]): Unit = HistologyEnricher(rc, studies).run()

  @main
  def specimen(rc: RuntimeETLContext, @arg(name = "study-id", short = 's', doc = "Study Id") studies: List[String]): Unit = SpecimenEnricher(rc, studies).run()

  @main
  def family(rc: RuntimeETLContext, @arg(name = "study-id", short = 's', doc = "Study Id") studies: List[String]): Unit = FamilyEnricher(rc, studies).run()

  @main
  def all(rc: RuntimeETLContext, @arg(name = "study-id", short = 's', doc = "Study Id") studies: List[String]): Unit = {
    HistologyEnricher(rc, studies).run()
    SpecimenEnricher(rc, studies).run()
    FamilyEnricher(rc, studies).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)

}
