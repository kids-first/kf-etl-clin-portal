package bio.ferlab.fhir.etl

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.reflect.ClassTag

object Utils {

  val actCodeR = "^phs[0-9.a-z]+"
  val studyCodePattern = "^SD_[0-9A-Za-z]+"

  private def codingSystemClassify(url: String) = {
    url match {
      case "http://purl.obolibrary.org/obo/mondo.owl" => "MONDO"
      case "https://www.who.int/classifications/classification-of-diseases" => "ICD"
      case "http://purl.obolibrary.org/obo/hp.owl" => "HPO"
      case "http://purl.obolibrary.org/obo/ncit.owl" => "NCIT"
      case _ => "Unknown"
    }
  }

  val extractAclFromList: UserDefinedFunction =
    udf((arr: Seq[String]) => arr.filter(e => (e matches actCodeR) || (e matches studyCodePattern)))

  val codingClassify: UserDefinedFunction =
    udf((arr: Seq[(String, String, String, String, String, String)]) => arr.map(r => (codingSystemClassify(r._2), r._4)))

  def firstNonNull: UserDefinedFunction = udf((arr: Seq[String]) => arr.find(_ != null).orNull)

  val ncitIdAnatomicalSite: UserDefinedFunction =
    udf((arr: Seq[(String, String, String, String, String, Boolean)]) => arr.find(r => r._2 matches "ncit\\.owl$") match {
      case Some(v) => v._4
      case None => null
    })

  val uberonIdAnatomicalSite: UserDefinedFunction =
    udf((arr: Seq[(String, String, String, String, String, Boolean)]) => arr.find(r => r._2 matches "uberon\\.owl$") match {
      case Some(v) => v._4
      case None => null
    })

  val extractHashes: UserDefinedFunction =
    udf(
      (arr: Seq[(Option[String], Seq[(Option[String], Option[String], Option[String], Option[String], Option[String], Option[Boolean])], Option[String])])
        => arr.map(r => r._2(0)._5 -> r._3).toMap)

  val retrieveIsHarmonized: UserDefinedFunction = udf((s: Option[String]) => s.exists(_.contains("harmonized-data")))
}
