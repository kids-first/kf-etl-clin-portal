package bio.ferlab.fhir.etl

import bio.ferlab.datalake.spark3.etl.{ETL, RawToNormalizedETL}
import bio.ferlab.datalake.spark3.public.SparkApp
import bio.ferlab.fhir.etl.centricTypes.ParticipantCentric

object PrepareIndex extends SparkApp {

  implicit val (conf, spark) = init()
  spark.sparkContext.setLogLevel("WARN")

  val participantCentric = new ParticipantCentric("re_00001")

  val data  = participantCentric.extract()
  val toto2  = participantCentric.transform(data)

//  runType match {
//    case "participants" => new ParticipantCentric(batchId).run()
//    case "all" =>
//      new ParticipantCentric(batchId, loadType).run()
//    case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
//  }
}
