package bio.ferlab.etl.normalized.dataservice

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import bio.ferlab.etl.normalized.dataservice.model.{ESequencingCenter, ESequencingExperiment}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import play.api.libs.ws.StandaloneWSClient
import play.api.libs.ws.ahc.StandaloneAhcWSClient

class EntityDataRetrieverTest extends AsyncFlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val wsClient: StandaloneWSClient = StandaloneAhcWSClient()

  override def afterAll(): Unit = {
    wsClient.close()
    system.terminate()
  }

  "retrieve" should "return the deserialize data on two pages" in {
    val handlerPage1 = jsonHandler(
      """
        |{
        |    "_links": {
        |        "self": "/seq_exp",
        |        "next": "/seq_exp2"
        |    },
        |    "_status": {
        |        "code": 200,
        |        "message": "success"
        |    },
        |    "limit": 1,
        |    "results": [
        |        {
        |            "kf_id": "1"
        |        }
        |    ],
        |    "total":2
        |}
      """.stripMargin)
    val handlerPage2 = jsonHandler(
      """
        |{
        |    "_links": {
        |        "self": "/seq_exp2"
        |    },
        |    "_status": {
        |        "code": 200,
        |        "message": "success"
        |    },
        |    "limit": 1,
        |    "results": [
        |        {
        |            "kf_id": "2"
        |        }
        | ],
        | "total":2
        |}
      """.stripMargin)
    DataService.withDataService(
      Map(
        "/seq_exp" -> handlerPage1,
        "/seq_exp2" -> handlerPage2
      )
    ) { url =>

      EntityDataRetriever(url)
        .retrieve[ESequencingExperiment]("/seq_exp")
        .map { r =>
          r shouldBe Seq(
            ESequencingExperiment(kf_id = Some("1")),
            ESequencingExperiment(kf_id = Some("2"))
          )
          handlerPage1.count shouldBe 1
          handlerPage2.count shouldBe 1
        }
    }
  }

  it should "return the deserialize data for sequencing-centers" in {
    val handler = jsonHandler(
      """
        |{
        |    "_links": {
        |        "self": "/sequencing-centers"
        |    },
        |    "_status": {
        |        "code": 200,
        |        "message": "success"
        |    },
        |    "limit": 100,
        |    "results": [
        |        {
        |            "_links": {
        |                "biospecimens": "/biospecimens?sequencing_center_id=SC_CATTVETT",
        |                "collection": "/sequencing-centers",
        |                "self": "/sequencing-centers/SC_CATTVETT",
        |                "sequencing_experiments": "/sequencing-experiments?sequencing_center_id=SC_CATTVETT"
        |            },
        |            "created_at": "2018-05-10T13:29:13.130932+00:00",
        |            "external_id": null,
        |            "kf_id": "SC_CATTVETT",
        |            "modified_at": "2018-05-10T13:29:13.130938+00:00",
        |            "name": "Feline Diagnostics LLC",
        |            "visible": true
        |        },
        |        {
        |            "_links": {
        |                "biospecimens": "/biospecimens?sequencing_center_id=SC_K52V7463",
        |                "collection": "/sequencing-centers",
        |                "self": "/sequencing-centers/SC_K52V7463",
        |                "sequencing_experiments": "/sequencing-experiments?sequencing_center_id=SC_K52V7463"
        |            },
        |            "created_at": "2018-05-22T21:26:58.414184+00:00",
        |            "external_id": null,
        |            "kf_id": "SC_K52V7463",
        |            "modified_at": "2018-05-22T21:26:58.414189+00:00",
        |            "name": "Washington University",
        |            "visible": true
        |        }
        |    ],
        |    "total": 2
        |}
        """.stripMargin
    )
    DataService.withDataService(Map("/sequencing-centers" -> handler)) { url =>

      EntityDataRetriever(url)
        .retrieve[ESequencingCenter]("/sequencing-centers").map { parsedSequencingCenters =>
        parsedSequencingCenters shouldBe Seq(
          ESequencingCenter(
            kf_id = Some("SC_CATTVETT"),
            name = Some("Feline Diagnostics LLC")
          ),
          ESequencingCenter(
            kf_id = Some("SC_K52V7463"),
            name = Some("Washington University")
          )
        )
      }
    }
  }


  it should "return the deserialize data after several retries" in {
    val handler = jsonHandlerAfterNRetries(
      """
        |{
        |    "_links": {
        |        "self": "/studies"
        |    },
        |    "_status": {
        |        "code": 200,
        |        "message": "success"
        |    },
        |    "limit": 10,
        |    "results": [
        |        {
        |            "kf_id": "1",
        |            "name": "Study 1"
        |        },
        |        {
        |            "kf_id": "2",
        |            "name": "Study 2"
        |        }
        | ],
        | "total":2
        |}
        """.stripMargin,
      2
    )
    DataService.withDataService(Map("/seq_exp" -> handler)) { url =>

      EntityDataRetriever(url)
        .retrieve[ESequencingExperiment]("/seq_exp", retries = 3)
        .map { r =>
          r shouldBe Seq(
            ESequencingExperiment(kf_id = Some("1")),
            ESequencingExperiment(kf_id = Some("2"))
          )
          handler.count shouldBe 2


        }
    }
  }
  //
  //  it should "return the deserialize data for biospecimen-diagnoses" in {
  //    val handler = jsonHandler(
  //      """
  //        |{
  //        |    "_links": {
  //        |        "self": "/studies"
  //        |    },
  //        |    "_status": {
  //        |        "code": 200,
  //        |        "message": "success"
  //        |    },
  //        |    "limit": 10,
  //        |    "results": [
  //        |        {
  //        |            "_links": {
  //        |                "biospecimen": "/biospecimens/BS_CGXTFM67",
  //        |                "collection": "/biospecimen-diagnoses",
  //        |                "diagnosis": "/diagnoses/DG_BXRD7128",
  //        |                "self": "/biospecimen-diagnoses/BD_EY46KMKQ"
  //        |            },
  //        |            "created_at": "2018-11-09T18:06:34.985858+00:00",
  //        |            "external_id": "BS_CGXTFM67:DG_BXRD7128",
  //        |            "kf_id": "BD_EY46KMKQ",
  //        |            "modified_at": "2019-07-18T20:48:24.668603+00:00",
  //        |            "visible": true
  //        |        },
  //        |        {
  //        |            "_links": {
  //        |                "biospecimen": "/biospecimens/BS_3Z40EZHD",
  //        |                "collection": "/biospecimen-diagnoses",
  //        |                "diagnosis": "/diagnoses/DG_PBK7GH8K",
  //        |                "self": "/biospecimen-diagnoses/BD_HTXNZM74"
  //        |            },
  //        |            "created_at": "2018-11-09T18:06:35.152377+00:00",
  //        |            "external_id": "BS_3Z40EZHD:DG_PBK7GH8K",
  //        |            "kf_id": "BD_HTXNZM74",
  //        |            "modified_at": "2019-07-18T20:48:25.111116+00:00",
  //        |            "visible": true
  //        |        }
  //        | ],
  //        | "total":2
  //        |}
  //      """.stripMargin
  //    )
  //    DataService.withDataService(Map("/biospecimen-diagnoses" -> handler)) { url =>
  //      val result = Await.result(
  //        EntityDataRetriever(DataServiceConfig(url, 100, "", ""))
  //          .retrieve[EBiospecimenDiagnosis]("/biospecimen-diagnoses"),
  //        60.seconds
  //      )
  //      result shouldBe Seq(
  //        EBiospecimenDiagnosis(
  //          kf_id = Some("BD_EY46KMKQ"),
  //          biospecimen_id = Some("BS_CGXTFM67"),
  //          diagnosis_id = Some("DG_BXRD7128"),
  //          visible = Some(true)
  //        ),
  //        EBiospecimenDiagnosis(
  //          kf_id = Some("BD_HTXNZM74"),
  //          biospecimen_id = Some("BS_3Z40EZHD"),
  //          diagnosis_id = Some("DG_PBK7GH8K"),
  //          visible = Some(true)
  //        )
  //      )
  //
  //    }
  //  }
  //
  //  "buildUrl" should "http://kf.org?study_id=1&visible=true&limit=100" in {
  //    buildUrl(
  //      "http://kf.org?study_id=1",
  //      Seq("visible=true")
  //    ) shouldBe "http://kf.org?study_id=1&visible=true&limit=100"
  //
  //  }
  //
  //  it should "http://kf.org?visible=true&limit=100" in {
  //    buildUrl(
  //      "http://kf.org",
  //      Seq("visible=true")
  //    ) shouldBe "http://kf.org?visible=true&limit=100"
  //  }
  //
  //  "entityDataExtractor" should "clean HPO Ids if not in a standard format" in {
  //    val hpoRegex = EntityDataExtractor.hpoRegex //Regex to test
  //    val phenotype1 = EPhenotype(
  //      kf_id = Some("id"),
  //      hpo_id_phenotype = Some("HP_12345"),
  //      participant_id = Some("part1")
  //    )
  //    val phenotype2 = EPhenotype(
  //      kf_id = Some("id"),
  //      hpo_id_phenotype = Some("HP:12345"),
  //      participant_id = Some("part1")
  //    )
  //
  //    val newPhenotype1 =
  //      phenotype1.copy(hpo_id_phenotype = phenotype1.hpo_id_phenotype match {
  //        case Some(p) => Some(hpoRegex.replaceFirstIn(p, ":"))
  //        case None    => None
  //      })
  //    val newPhenotype2 =
  //      phenotype2.copy(hpo_id_phenotype = phenotype2.hpo_id_phenotype match {
  //        case Some(p) => Some(hpoRegex.replaceFirstIn(p, ":"))
  //        case None    => None
  //      })
  //
  //    newPhenotype1.hpo_id_phenotype.getOrElse("") should equal("HP:12345")
  //    newPhenotype2.hpo_id_phenotype.getOrElse("") should equal("HP:12345")
  //  }
  //
  //  "dataService" should "return the deserialize data for sequencing-centers" in {
  //    val handler = jsonHandler(
  //      """
  //        |{
  //        |    "_links": {
  //        |        "self": "/sequencing-centers"
  //        |    },
  //        |    "_status": {
  //        |        "code": 200,
  //        |        "message": "success"
  //        |    },
  //        |    "limit": 100,
  //        |    "results": [
  //        |        {
  //        |            "_links": {
  //        |                "biospecimens": "/biospecimens?sequencing_center_id=SC_CATTVETT",
  //        |                "collection": "/sequencing-centers",
  //        |                "self": "/sequencing-centers/SC_CATTVETT",
  //        |                "sequencing_experiments": "/sequencing-experiments?sequencing_center_id=SC_CATTVETT"
  //        |            },
  //        |            "created_at": "2018-05-10T13:29:13.130932+00:00",
  //        |            "external_id": null,
  //        |            "kf_id": "SC_CATTVETT",
  //        |            "modified_at": "2018-05-10T13:29:13.130938+00:00",
  //        |            "name": "Feline Diagnostics LLC",
  //        |            "visible": true
  //        |        },
  //        |        {
  //        |            "_links": {
  //        |                "biospecimens": "/biospecimens?sequencing_center_id=SC_K52V7463",
  //        |                "collection": "/sequencing-centers",
  //        |                "self": "/sequencing-centers/SC_K52V7463",
  //        |                "sequencing_experiments": "/sequencing-experiments?sequencing_center_id=SC_K52V7463"
  //        |            },
  //        |            "created_at": "2018-05-22T21:26:58.414184+00:00",
  //        |            "external_id": null,
  //        |            "kf_id": "SC_K52V7463",
  //        |            "modified_at": "2018-05-22T21:26:58.414189+00:00",
  //        |            "name": "Washington University",
  //        |            "visible": true
  //        |        }
  //        |    ],
  //        |    "total": 2
  //        |}
  //      """.stripMargin
  //    )
  //    DataService.withDataService(Map("/sequencing-centers" -> handler)) { url =>
  //      val parsedSequencingCenters = Await.result(
  //        EntityDataRetriever(DataServiceConfig(url, 100, "", ""))
  //          .retrieve[ESequencingCenter]("/sequencing-centers"),
  //        60.seconds
  //      )
  //
  //      parsedSequencingCenters shouldBe Seq(
  //        ESequencingCenter(
  //          kf_id = Some("SC_CATTVETT"),
  //          name = Some("Feline Diagnostics LLC")
  //        ),
  //        ESequencingCenter(
  //          kf_id = Some("SC_K52V7463"),
  //          name = Some("Washington University")
  //        )
  //      )
  //    }
  //  }
  //  implicit val defaultPatience =
  //    PatienceConfig(timeout = Span(5, Seconds), interval = Span(5000, Millis))
  //
  //  "retrieveAclForDCFFiles" should "return the deserialize data" in {
  //    EntityDataRetriever(DataServiceConfig("", 100, "", ""))
  //      .retrieveAclForDCFFiles("https://nci-crdc.datacommons.io/index/index/e5d7fd85-e3fd-4b79-bf91-7258e2124a84" )
  //      .get shouldEqual Seq("phs000218")
  //  }
}
