package bio.ferlab.etl.normalized.clinical

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{Column, functions}

object Utils {
  val extractSystemUrl = "^(http[s]?:\\/\\/[A-Za-z0-9-.\\/]+)\\/[A-za-z0-9-?.]+[a-z\\/=]{1}$"
  val phenotypeExtract = "^[A-Z]{2,}.[0-9]+$"
  val DRS_HOSTNAME = "drs://data.kidsfirstdrc.org/"
  val virtualBiorepositoryContact = "Virtual Biorepository Contact"

  private def codingSystemClassify(url: String) = {
    url match {
      case "http://purl.obolibrary.org/obo/mondo.owl" => "MONDO"
      case "https://www.who.int/classifications/classification-of-diseases" => "ICD"
      case "http://purl.obolibrary.org/obo/hp.owl" => "HPO"
      case "http://purl.obolibrary.org/obo/ncit.owl" => "NCIT"
      case _ => "Unknown"
    }
  }

  def extractLatestDid(urlCol: Column): Column = when(urlCol.startsWith(DRS_HOSTNAME), DRS_HOSTNAME).otherwise("")

  def extractFirstMatchingSystem(column: Column, systemUrls: Seq[String]): Column = filter(column, c => c("system").isin(systemUrls: _*))(0)

  val extractDocumentReferenceAcl: (Column, Column) => Column = (cFileAcl: Column, cStudyId: Column) => {
    val openAcl = Seq("*", "Registered")
    val xs = filter(cFileAcl, x => x.startsWith("phs") || x.isin(openAcl: _*) || x === cStudyId)
    val acl = transform(xs, x => when(x.isin(openAcl: _*), lit("open_access")).otherwise(x))
    array_distinct(acl)
  }
  val extractReferencesId: Column => Column = (column: Column) => functions.transform(column, extractReferenceId)

  val extractReferenceId: Column => Column = (column: Column) => functions.split(column, "/")(1)

  val extractFirstForSystem: (Column, Seq[String]) => Column = (column: Column, system: Seq[String]) => filter(column, c => regexp_extract(c("system"), extractSystemUrl, 1).isin(system: _*))(0)

  val extractDocUrl: Column => Column = artifacts => filter(artifacts, c => c("type") === "documentation")(0)

  val extractVirtualBiorepositoryContact: Column => Column = contacts => filter(contacts, c => c("name") === virtualBiorepositoryContact)(0)

  val firstSystemEquals: (Column, String) => Column = (column: Column, system: String) => filter(column, c => c("system") === system)(0)

  val extractOfficial: Column => Column = (identifiers: Column) => coalesce(extractIdentifierForUse(identifiers, "official"), identifiers(0)("value"))

  val extractIdentifierForUse: (Column, String) => Column = (identifiers: Column, use: String) => filter(identifiers, identifier => identifier("use") === use)(0)("value")

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

  val gen3Domain = "data.kidsfirstdrc.org"
  val dcfDomain = "nci-crdc.datacommons.io"
  val dcfDomain2 = "api.gdc.cancer.gov"
  val retrieveRepository: Column => Column = url =>
    when(url like s"%$gen3Domain%", "gen3")
      .when(url like s"%$dcfDomain%", "dcf")
      //legacy test
      .when(url like s"%$dcfDomain2%", "dcf")
      .otherwise(null)

  val retrieveSize: Column => Column = rawFileSize => rawFileSize.cast(StringType).cast(LongType)

  val extractStudyVersion: Column => Column = rawVersion =>
    when(rawVersion.isNull, lit(null))
      .otherwise(concat_ws(".", slice(split(rawVersion, "\\."), 2, Short.MaxValue)))

  val extractStudyExternalId: Column => Column = rawExternalId => split(rawExternalId, "\\.")(0)

  val sanitizeFilename: Column => Column = fileName => slice(split(fileName, "/"), -1, 1)(0)

  val age_on_set: (Column, Seq[(Int, Int)]) => Column = (c, intervals) => {
    val (_, lastHigh) = intervals.last
    intervals.foldLeft(when(c > lastHigh, s"$lastHigh+")) { case (column, (low, high)) =>
      column.when(c >= low && c < high, s"$low - $high")
    }
  }

  val upperFirstLetter: Column => Column = c => concat(upper(substring(c, 1, 1)), lower(substring(c, 2, 10000)))

  val patternPractitionerRoleResearchStudy = "PractitionerRole\\/([0-9]+)"

  val officialIdentifier: Column = extractOfficial(col("identifier"))

  val age_at_bio_collection_on_set_intervals: Seq[(Int, Int)] = Seq((0, 5), (5, 10), (10, 20), (20, 30), (30, 40), (40, 50), (50, 60), (60, 70), (70, 80))

}
