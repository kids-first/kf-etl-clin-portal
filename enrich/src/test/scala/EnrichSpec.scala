import bio.ferlab.enrich.etl.SpecimenEnricher.specimensToHistoPathologies
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EnrichSpec extends AnyFlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  it should "enrich specimens with pathologies" in {
    val histJson = """{"subject": {"reference": "Patient/403111"},"focus": [{"reference": "Condition/471979"}],"specimen":{"reference": "Specimen/659105"}}"""
    val mondoJson = """{"id": "http://localhost:8000/Condition/471979/_history/4","code": {"coding": [ {"system": "http://purl.obolibrary.org/obo/mondo.owl","code": "MONDO:0004933"} ],"text": "Hypoplastic left heart syndrome"}}}"""
    val ncitJson = """{"id": "http://localhost:8000/Condition/471979/_history/4","code": {"coding": [ { "system": "http://purl.obolibrary.org/obo/mondo.owl","code": "MONDO:0005711"}, {"system": "http://purl.obolibrary.org/obo/ncit.owl","code": "NCIT:C98893"}], "text": "congential diaphragmatic hernia"}}}"""


    val histDf = spark.read.json(Seq(histJson).toDS())
    val mondoDf = spark.read.json(Seq(mondoJson).toDS())
    val ncitDf = spark.read.json(Seq(ncitJson).toDS())


    val joined = specimensToHistoPathologies(histDf, mondoDf, ncitDf)
    joined.count equals 1
  }
}

