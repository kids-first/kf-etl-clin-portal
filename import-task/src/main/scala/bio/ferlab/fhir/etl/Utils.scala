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

//  def firstNonNull[T >: Null : ClassTag]: UserDefinedFunction = udf((arr: Seq[T]) => arr.find(_ != null).orNull)
  def firstNonNull: UserDefinedFunction = udf((arr: Seq[String]) => arr.find(_ != null).orNull)
  def firstNonNullDecimal: UserDefinedFunction = udf((arr: Seq[BigDecimal]) => arr.find(_ != null).orNull)
//  def firstNonNullFloat: UserDefinedFunction = udf((arr: Seq[Float]) => arr.find(_ != null).orNull)

//  val firstNonNull: UserDefinedFunction =
//    udf((arr: Seq[String]) => arr.find(_ != null).orNull)


//  val firstNonNullr: UserDefinedFunction =
//    udf((arr: Seq[Numeric]) => arr.find(_ != null).orNull)

}
