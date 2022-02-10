package bio.ferlab.fhir.etl

import bio.ferlab.fhir.etl.ImportTask.expReleaseId
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, filter, regexp_extract, udf}
import org.apache.spark.sql.{Column, functions}

object Utils {

  val actCodeR = "^phs[0-9.a-z]+"
  val extractSystemUrl = "^(http[s]?:\\/\\/[A-Za-z0-9-.\\/]+)\\/[A-za-z0-9-]+[a-z\\/]{1}$"
  val gen3Host = "data.kidsfirstdrc.org"
  val dcfHost = "api.gdc.cancer.gov"
  val patternUrnUniqueIdStudy = "[A-Z][a-z]+-(SD_[0-9A-Za-z]+)-([A-Z]{2}_[0-9A-Za-z]+)"


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
    udf((arr: Seq[String]) => arr.filter(e => (e matches actCodeR) || (e == expReleaseId) ))

  val extractReferencesId: Column => Column = (column: Column) => functions.transform(column, c => functions.split(c, "/")(1))

  val extractReferenceId: Column => Column = (column: Column) => functions.split(column, "/")(1)

  val extractStudyId: () => Column = () => regexp_extract(extractFirstForSystem(col("identifier"), Seq(URN_UNIQUE_ID))("value"), patternUrnUniqueIdStudy, 1)

//  val extractFirstForSystem = (column: Column, system: Seq[String]) => filter(column, c => c("system") === system)(0)
  val extractFirstForSystem = (column: Column, system: Seq[String]) => filter(column, c => regexp_extract(c("system"), extractSystemUrl, 1).isin(system: _*))(0)

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
        => arr.map(r => r._2.head._5 -> r._3).toMap)

  val retrieveIsHarmonized: UserDefinedFunction = udf((s: Option[String]) => s.exists(_.contains("harmonized-data")))

  val retrieveIsControlledAccess: UserDefinedFunction = udf((s: Option[String]) => {
    if(s.exists(_.equals("R"))) {
      "Controlled"
    } else {
      "Open"
    }
  })

  val retrieveRepository: UserDefinedFunction = udf((s: Option[String]) => {
    if(s.exists(_.contains(gen3Host))) {
      "gen3"
    }
    else if (s.exists(_.contains(dcfHost))) {
      "dcf"
    } else {
      null
    }
  })

  val retrieveSize: UserDefinedFunction = udf((d: Option[String]) => d.map(BigInt(_).toLong))

  val extractStudyVersion: UserDefinedFunction = udf((s: Option[String]) => s.map(_.split('.').tail.mkString(".")))

  val extractStudyExternalId: UserDefinedFunction = udf((s: Option[String]) => s.map(_.split('.').head))
}
