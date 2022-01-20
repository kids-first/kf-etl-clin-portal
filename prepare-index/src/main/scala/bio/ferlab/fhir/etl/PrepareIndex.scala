package bio.ferlab.fhir.etl

import bio.ferlab.datalake.spark3.public.SparkApp
import bio.ferlab.fhir.etl.centricTypes.{BiospecimenCentric, FileCentric, ParticipantCentric, SimpleParticipant, StudyCentric}

object PrepareIndex extends SparkApp {

  implicit val (conf, _, spark) = init()
  spark.sparkContext.setLogLevel("WARN")

  val studyCentric = new StudyCentric("re_00001")
  val studyCentricInput = studyCentric.extract()
  val studyCentricDf = studyCentric.transform(studyCentricInput)
  studyCentric.load(studyCentricDf)

  val simpleParticipant = new SimpleParticipant("re_00001")
  val simpleParticipantInput = simpleParticipant.extract()
  val simpleParticipantDf = simpleParticipant.transform(simpleParticipantInput)
  simpleParticipant.load(simpleParticipantDf)

  val fileCentric = new FileCentric("re_00001")
  val fileInput = fileCentric.extract()
  val fileCentricDf = fileCentric.transform(fileInput)
  fileCentric.load(fileCentricDf)

  val biospecimenCentric = new BiospecimenCentric("re_00001")
  val biospecimenCentricInput = biospecimenCentric.extract()
  val biospecimenCentricDf = biospecimenCentric.transform(biospecimenCentricInput)
  biospecimenCentric.load(biospecimenCentricDf)

  val participantCentric = new ParticipantCentric("re_00001")
  val participantCentricInput = participantCentric.extract()
  val participantCentricDf = participantCentric.transform(participantCentricInput)
  participantCentric.load(participantCentricDf)


  //  runType match {
  //    case "participants" => new ParticipantCentric(batchId).run()
  //    case "all" =>
  //      new ParticipantCentric(batchId, loadType).run()
  //    case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
  //  }
}
