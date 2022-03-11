package bio.ferlab.fhir.etl

//import bio.ferlab.fhir.etl.ImportTask.expReleaseId

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{coalesce, col, exists, filter, last, lit, regexp_extract, regexp_replace, slice, split, udf, when}
import org.apache.spark.sql.{Column, functions}

object Utils {

  val actCodeR = "^phs[0-9.a-z]+"
  val extractSystemUrl = "^(http[s]?:\\/\\/[A-Za-z0-9-.\\/]+)\\/[A-za-z0-9-?.]+[a-z\\/=]{1}$"
  val gen3Host = "data.kidsfirstdrc.org"
  val dcfHost = "api.gdc.cancer.gov"
  val patternUrnUniqueIdStudy = "[A-Z][a-z]+-(SD_[0-9A-Za-z]+)-([A-Z]{2}_[0-9A-Za-z]+)"
  val documentReferenceExtract = "^DocumentReference\\/([0-9]+)$"
  val specimenExtract = "^Specimen\\/([0-9]+)$"
  val phenotypeExtract = "^[A-Z]{2,}.[0-9]+$"


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
    udf((arr: Seq[String], studyId: String) => arr.filter(e => (e matches actCodeR) || (e == studyId)))

  val extractReferencesId: Column => Column = (column: Column) => functions.transform(column, c => functions.split(c, "/")(1))

  val extractReferenceId: Column => Column = (column: Column) => functions.split(column, "/")(1)

  val extractStudyId: () => Column = () => regexp_extract(extractFirstForSystem(col("identifier"), Seq(URN_UNIQUE_ID))("value"), patternUrnUniqueIdStudy, 1)

  val extractFirstForSystem: (Column, Seq[String]) => Column = (column: Column, system: Seq[String]) => filter(column, c => regexp_extract(c("system"), extractSystemUrl, 1).isin(system: _*))(0)

  val extractOfficial: Column => Column = (identifiers: Column) => coalesce(filter(identifiers, identifier => identifier("use") === "official")(0)("value"), identifiers(0)("value"))


  val codingClassify: UserDefinedFunction =
    udf((arr: Seq[(String, String, String, String, String, String)]) =>
      arr.map(
        r => (codingSystemClassify(r._2),
          if (r._4.matches(phenotypeExtract)) r._4.replace("_", ":") else r._4)
      )
    )

  def firstNonNull: Column => Column = arr => filter(arr, a => a.isNotNull)(0)

  val extractHashes: UserDefinedFunction =
    udf(
      (arr: Seq[(Option[String], Seq[(Option[String], Option[String], Option[String], Option[String], Option[String], Option[Boolean])], Option[String])])
      => arr.map(r => r._2.head._5 -> r._3).toMap)

  val retrieveIsHarmonized: Column => Column = url => url.isNotNull && (url like "harmonized-data")

  val retrieveRepository: Column => Column = url => when(url like s"%$gen3Host%", "gen3")
    .when(url like s"%$dcfHost%", "dcf")
    .otherwise(null)

  val retrieveSize: UserDefinedFunction = udf((d: Option[String]) => d.map(BigInt(_).toLong))

  val extractStudyVersion: UserDefinedFunction = udf((s: Option[String]) => s.map(_.split('.').tail.mkString(".")))

  val extractStudyExternalId: UserDefinedFunction = udf((s: Option[String]) => s.map(_.split('.').head))

  val sanitizeFilename: Column => Column = fileName => slice(split(fileName, "/"), -1, 1)(0)
}
