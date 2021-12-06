package bio.ferlab.fhir.etl

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.reflect.ClassTag

object Utils {

  val actCodeR = "^phs[0-9.a-z]+"

  private def codingSystemClassify(url : String) = {
    url match {
      case "http://purl.obolibrary.org/obo/mondo.owl" => "MONDO"
      case "https://www.who.int/classifications/classification-of-diseases" => "ICD"
      case "http://purl.obolibrary.org/obo/hp.owl" => "HPO"
      case _ => "Unknown" //TODO determine other types  NCIT???
    }
  }

  val extractAclFromList: UserDefinedFunction =
    udf((arr: Seq[String]) => arr.filter(e => e matches actCodeR))

  val codingClassify: UserDefinedFunction =
    udf((arr: Seq[(String, String, String, String, String, String)]) => arr.map(r => (codingSystemClassify(r._2), r._4)))

  def firstNonNull: UserDefinedFunction = udf((arr: Seq[String]) => arr.find(_ != null).orNull)

  val ncitIdAnatomicalSite: UserDefinedFunction =
    udf((arr: Seq[(String, String, String, String, String, Boolean)]) => arr.find(r => r._2 matches "ncit\\.owl$" ) match {
      case Some(v) => v._4
      case None => null
    })

  val uberonIdAnatomicalSite: UserDefinedFunction =
    udf((arr: Seq[(String, String, String, String, String, Boolean)]) => arr.find(r => r._2 matches "uberon\\.owl$" ) match {
      case Some(v) => v._4
      case None => null
    })

}
