package bio.ferlab.etl.prepared

import bio.ferlab.datalake.spark3.SparkApp
import bio.ferlab.datalake.spark3.genomics.prepared.{GeneCentric, GenesSuggestions, VariantCentric, VariantsSuggestions}

object RunPrepared extends SparkApp {

  val Array(_, _, jobName) = args

  implicit val (conf, steps, spark) = init(appName = s"Prepare $jobName")

  log.info(s"Job: $jobName")
  log.info(s"runType: ${steps.mkString(" -> ")}")
  jobName match {
    case "variant_centric" => new VariantCentric().run(steps)
    case "gene_centric" => new GeneCentric().run(steps)
    case "variant_suggestions" => new VariantsSuggestions().run(steps)
    case "gene_suggestions" => new GenesSuggestions().run(steps)
    case s: String => throw new IllegalArgumentException(s"jobName [$s] unknown.")
  }

}