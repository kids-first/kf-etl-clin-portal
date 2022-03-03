package bio.ferlab.fhir.etl

import bio.ferlab.datalake.spark3.public.SparkApp
import bio.ferlab.fhir.etl.centricTypes.{BiospecimenCentric, FileCentric, ParticipantCentric, SimpleParticipant, StudyCentric}

object PrepareIndex extends SparkApp {
  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(_, _, jobName, releaseId, studyIds) = args

  implicit val (conf, _, spark) = init()

  spark.sparkContext.setLogLevel("WARN")

  val studyList = studyIds.split(",").toList

  jobName match {
    case "study_centric" => new StudyCentric(releaseId, studyList).run()
    case "participant_centric" =>
      new StudyCentric(releaseId, studyList).run()
      new SimpleParticipant(releaseId, studyList).run()
      new ParticipantCentric(releaseId, studyList).run()
    case "file_centric" =>
      new StudyCentric(releaseId, studyList).run()
      new SimpleParticipant(releaseId, studyList).run()
      new FileCentric(releaseId, studyList).run()
    case "biospecimen_centric" =>
      new StudyCentric(releaseId, studyList).run()
      new SimpleParticipant(releaseId, studyList).run()
      new BiospecimenCentric(releaseId, studyList).run()
    case "all" =>
      new StudyCentric(releaseId, studyList).run()
      new SimpleParticipant(releaseId, studyList).run()
      new ParticipantCentric(releaseId, studyList).run()
      new FileCentric(releaseId, studyList).run()
      new BiospecimenCentric(releaseId, studyList).run()
  }
}
