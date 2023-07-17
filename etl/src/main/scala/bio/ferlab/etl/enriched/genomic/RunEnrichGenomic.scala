package bio.ferlab.etl.enriched.genomic

import bio.ferlab.datalake.spark3.SparkApp
import bio.ferlab.datalake.spark3.genomics.enriched.{Consequences, Variants}
import bio.ferlab.datalake.spark3.genomics.{AtLeastNElements, FirstElement, FrequencySplit, SimpleAggregation}
import bio.ferlab.etl.Constants.columns.{TRANSMISSIONS, TRANSMISSION_MODE}
import org.apache.spark.sql.functions.col

object RunEnrichGenomic extends SparkApp {

  val Array(_, _, jobName) = args

  implicit val (conf, steps, spark) = init(appName = s"Enrich $jobName")

  log.info(s"Job: $jobName")
  log.info(s"runType: ${steps.mkString(" -> ")}")
  private val variants = new Variants(snvDatasetId = "normalized_snv",frequencies = Seq(
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
    FrequencySplit("internal_frequencies", byAffected = false)))
  jobName match {
    case "variants" => variants.run(steps)
    case "consequences" => new Consequences().run(steps)
    case "all" =>
      variants.run(steps)
      new Consequences().run(steps)
    case s: String => throw new IllegalArgumentException(s"jobName [$s] unknown.")
  }

}