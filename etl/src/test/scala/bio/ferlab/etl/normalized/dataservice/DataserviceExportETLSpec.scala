package bio.ferlab.etl.normalized.dataservice

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import bio.ferlab.datalake.commons.config.SimpleConfiguration
import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.etl.normalized.dataservice.model.ESequencingExperiment
import bio.ferlab.etl.testutils.{KFTestETLContext, WithTestConfig}
import bio.ferlab.fhir.etl.config.ETLConfiguration
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import play.api.libs.ws.StandaloneWSClient
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.Future

class DataserviceExportETLSpec extends AnyFlatSpec with Matchers with WithSparkSession with WithTestConfig with BeforeAndAfterAll {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val wsClient: StandaloneWSClient = StandaloneAhcWSClient()

  import scala.concurrent.ExecutionContext.Implicits._

  override def afterAll(): Unit = {
    wsClient.close()
    system.terminate()
  }

  val study1SeqExp: Seq[ESequencingExperiment] = Seq(ESequencingExperiment(kf_id = Some("seq_exp_1")), ESequencingExperiment(kf_id = Some("seq_exp_2")))
  val study2SeqExp: Seq[ESequencingExperiment] = Seq(ESequencingExperiment(kf_id = Some("seq_exp_3")), ESequencingExperiment(kf_id = Some("seq_exp_4")))
  implicit val c: ETLConfiguration = conf

  object fakeEntityRetriever extends DataRetriever {
    override def retrieve[T](endpoint: String, data: Seq[T], retries: Int)(implicit extractor: EntityDataExtractor[T]): Future[Seq[T]] = {
      if (endpoint.contains("sequencing-experiments")) {
        Future {
          if (endpoint.contains("sd_1")) {
            study1SeqExp.map(_.asInstanceOf[T])

          } else if (endpoint.contains("sd_2")) {
            study2SeqExp.map(_.asInstanceOf[T])
          } else {
            Seq.empty[T]
          }
        }
      }
      else {
        Future(Seq.empty[T])
      }

    }
  }

   // You can run this test locally, but it fails in github actions.
   ignore should "return dataframes for all studies when extracting" in {
    import spark.implicits._
    val etl = new DataserviceExportETL(KFTestETLContext(), "re_0001", List("sd_1", "sd_2"), fakeEntityRetriever)
    val results = etl.extract()
    val normalized_sequencing_experiment = results.get("normalized_sequencing_experiment")
    normalized_sequencing_experiment shouldBe defined
    normalized_sequencing_experiment.get.select("kf_id").as[String].collect() should contain theSameElementsAs Seq("seq_exp_1", "seq_exp_2", "seq_exp_3", "seq_exp_4")
  }
}
