package bio.ferlab.etl.enriched.clinical

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import bio.ferlab.etl.mainutils.Studies
import mainargs.{ParserForMethods, main}

object RunEnrichClinical {

  @main
  def histology(rc: RuntimeETLContext, studies: Studies): Unit = HistologyEnricher(rc, studies.ids).run()

  @main
  def specimen(rc: RuntimeETLContext, studies: Studies): Unit = SpecimenEnricher(rc, studies.ids).run()

  @main
  def family(rc: RuntimeETLContext, studies: Studies): Unit = FamilyEnricher(rc, studies.ids).run()

  @main
  def all(rc: RuntimeETLContext, studies: Studies): Unit = {
    HistologyEnricher(rc, studies.ids).run()
    SpecimenEnricher(rc, studies.ids).run()
    FamilyEnricher(rc, studies.ids).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)

}

