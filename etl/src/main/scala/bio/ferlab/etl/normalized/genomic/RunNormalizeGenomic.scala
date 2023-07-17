package bio.ferlab.etl.normalized.genomic

import bio.ferlab.datalake.spark3.SparkApp

object RunNormalizeGenomic extends SparkApp {
  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(_, _, jobName, studyId, releaseId, vcfPattern, referenceGenomePath) = args

  implicit val (conf, _, spark) = init()

  spark.sparkContext.setLogLevel("WARN")

  jobName match {
    case "snv" => new SNV(studyId, releaseId, vcfPattern, Some(referenceGenomePath)).run()
    case "consequences" => new Consequences(studyId, vcfPattern, Some(referenceGenomePath)).run()
  }

}