package bio.ferlab.fhir.etl

import bio.ferlab.datalake.spark3.public.SparkApp
import bio.ferlab.fhir.etl.centricTypes.{BiospecimenCentric, FileCentric, ParticipantCentric, SimpleParticipant, StudyCentric}

object PrepareIndex extends SparkApp {
  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(_, _, jobName, releaseId) = args

  implicit val (conf, _, spark) = init()

  spark.sparkContext.setLogLevel("WARN")

  jobName match {
    case "study_centric" => new StudyCentric(releaseId).run()
    case "file_centric" => {
      new SimpleParticipant(releaseId).run()
      new FileCentric(releaseId).run()
    }
    case "participant_centric" => {
      new SimpleParticipant(releaseId).run()
      new ParticipantCentric(releaseId).run()
    }
    case "file_centric" => {
      new SimpleParticipant(releaseId).run()
      new FileCentric(releaseId).run()
    }
    case "biospecimen_centric" => {
      new SimpleParticipant(releaseId).run()
      new BiospecimenCentric(releaseId).run()
    }
    case "all" => {
      new StudyCentric(releaseId).run()
      new SimpleParticipant(releaseId).run()
      new FileCentric(releaseId).run()
      new ParticipantCentric(releaseId).run()
      new FileCentric(releaseId).run()
      new BiospecimenCentric(releaseId).run()
    }
  }
}
