package bio.ferlab.etl.enriched.genomic

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import bio.ferlab.datalake.spark3.genomics.enriched.{Consequences, Variants}
import bio.ferlab.datalake.spark3.genomics.{AtLeastNElements, FirstElement, FrequencySplit, SimpleAggregation}
import bio.ferlab.etl.Constants.columns.{TRANSMISSIONS, TRANSMISSION_MODE}
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions.{array_contains, col}

object RunEnrichGenomic {

  @main
  def snv(rc: RuntimeETLContext): Unit = variants(rc).run()

  @main
  def consequences(rc: RuntimeETLContext): Unit = Consequences(rc).run()

  @main
  def all(rc: RuntimeETLContext): Unit = {
    snv(rc)
    consequences(rc)
  }

  private def variants(rc: RuntimeETLContext) = Variants(rc, snvDatasetId = "normalized_snv",
    frequencies = Seq(
      FrequencySplit(
        "studies",
        splitBy = Some(col("study_id")), byAffected = false,
        extraAggregations = Seq(
          AtLeastNElements(name = "participant_ids", c = col("participant_id"), n = 10),
          SimpleAggregation(name = TRANSMISSIONS, c = col(TRANSMISSION_MODE)),
          SimpleAggregation(name = "zygosity", c = col("zygosity")),
          FirstElement(name = "study_code", c = col("study_code"))
        )
      ),
      FrequencySplit("internal_frequencies", byAffected = false)
    ),
    filterSnv = Some(col("has_alt") && array_contains(col("filters"), "PASS"))
  )

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)


}