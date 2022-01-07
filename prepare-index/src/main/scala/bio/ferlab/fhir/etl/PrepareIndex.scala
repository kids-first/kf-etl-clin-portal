package bio.ferlab.fhir.etl

import bio.ferlab.datalake.spark3.public.SparkApp
import bio.ferlab.fhir.etl.centricTypes.{CommonParticipant, FileCentric, ParticipantCentric, StudyCentric}

object PrepareIndex extends SparkApp {

  implicit val (conf, _, spark) = init()
  spark.sparkContext.setLogLevel("WARN")

  val commonParticipant = new CommonParticipant("re_00001").run()
  //  val commonParticipantInput  = commonParticipant.extract()
  //  val commonParticipantDf  = commonParticipant.transform(commonParticipantInput)
  //  commonParticipant.load(commonParticipantDf)

  val studyCentric = new StudyCentric("re_00001").run()
  //  val studyCentricInput  = studyCentric.extract()
  //  val studyCentricDf  = studyCentric.transform(studyCentricInput)
  //  studyCentric.load(studyCentricDf)

  val participantCentric = new ParticipantCentric("re_00001").run()
//  val participantCentricInput  = participantCentric.extract()
//  val participantCentricDf  = participantCentric.transform(participantCentricInput)
//  participantCentric.load(participantCentricDf)

  val fileCentric = new FileCentric("re_00001").run()
//  val fileInput  = fileCentric.extract()
//  val fileCentricDf  = fileCentric.transform(fileInput)



//  runType match {
//    case "participants" => new ParticipantCentric(batchId).run()
//    case "all" =>
//      new ParticipantCentric(batchId, loadType).run()
//    case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
//  }
}
