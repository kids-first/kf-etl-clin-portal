package bio.ferlab.etl.prepared.genomic

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import bio.ferlab.datalake.spark3.genomics.prepared.{GeneCentric, GenesSuggestions, VariantCentric, VariantsSuggestions}
import mainargs.{ParserForMethods, main}

object RunPrepareGenomic {

  @main
  def variant_centric(rc: RuntimeETLContext): Unit = VariantCentric(rc).run()

  @main
  def gene_centric(rc: RuntimeETLContext): Unit = GeneCentric(rc).run()

  @main
  def variant_suggestions(rc: RuntimeETLContext): Unit = VariantsSuggestions(rc).run()

  @main
  def gene_suggestions(rc: RuntimeETLContext): Unit = GenesSuggestions(rc).run()


  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)
}