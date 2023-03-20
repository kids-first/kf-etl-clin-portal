package bio.ferlab.fhir.etl

import bio.ferlab.datalake.spark3.SparkApp
import bio.ferlab.fhir.etl.centricTypes.{BiospecimenCentric, FileCentric, ParticipantCentric, SimpleParticipant, StudyCentric}

object PrepareIndex extends SparkApp {
  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(_, _, jobName, studyIds) = args

  implicit val (conf, _, spark) = init()

  spark.sparkContext.setLogLevel("WARN")

  val studyList = studyIds.split(",").toList

  jobName match {
    case "study_centric" => new StudyCentric(studyList).run()
    case "participant_centric" =>
      new StudyCentric(studyList).run()
      new SimpleParticipant(studyList).run()
      new ParticipantCentric(studyList).run()
    case "file_centric" =>
      new StudyCentric(studyList).run()
      new SimpleParticipant(studyList).run()
      new FileCentric(studyList).run()
    case "biospecimen_centric" =>
      new StudyCentric(studyList).run()
      new SimpleParticipant(studyList).run()
      new BiospecimenCentric(studyList).run()
    case "all" =>
      new StudyCentric(studyList).run()
      new SimpleParticipant(studyList).run()
      new ParticipantCentric(studyList).run()
      new FileCentric(studyList).run()
      new BiospecimenCentric(studyList).run()
  }
}
