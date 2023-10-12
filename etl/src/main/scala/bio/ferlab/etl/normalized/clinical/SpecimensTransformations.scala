package bio.ferlab.etl.normalized.clinical

import bio.ferlab.datalake.spark3.transformation.{Custom, Drop, Transformation}
import bio.ferlab.etl.normalized.clinical.Utils._
import bio.ferlab.etl.normalized.clinical.clinical._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object SpecimensTransformations {


  private def extractSecondaryIdentifier(xs: Column, systemParam: String): Column =
    filter(xs, x => x("use") === "secondary" && x("system").contains(systemParam))(0)("value")

  private def extractNcitAnatomySiteId(xs: Column): Column = filter(xs, x => x("code").startsWith("NCIT:"))(0)("code")

  private def extractConsentType(keyword: String): Column = filter(col("meta")("security"), x => x("system").contains(keyword))(0)("code")

  private def addParentsToSpecimen(specimen: DataFrame): DataFrame = {
    val parentRange = 1 to 10
    val samplesWithParent = parentRange.foldLeft(specimen) { case (s, i) =>
        val joined = specimen.select(struct(col("fhir_id"), col("sample_id"), col("parent_id"), col("sample_type"), lit(i) as "level") as s"parent_$i")
        s.join(joined, s(s"parent_${i - 1}.parent_id") === joined(s"parent_$i.fhir_id"), "left")
      }
      .withColumn("parent_sample_type", col("parent_1.sample_type"))
      .withColumn("parent_sample_id", col("parent_1.sample_id"))
      .withColumn("parent_fhir_id", col("parent_1.fhir_id"))
      .withColumn("external_parent_sample_id", col("parent_1.external_sample_id"))
      .withColumn("collection_sample", coalesce(parentRange.reverse.map(p => col(s"parent_$p")): _*))
      .withColumn("collection_sample_id", col("collection_sample.sample_id"))
      .withColumn("collection_sample_type", col("collection_sample.sample_type"))
      .withColumn("collection_fhir_id", col("collection_sample.fhir_id"))
      .withColumn("external_collection_sample_id", col("collection_sample.external_sample_id"))
    samplesWithParent
      .where(col("collection_fhir_id") =!= col("fhir_id"))
      .drop(parentRange.map(p => s"parent_$p"): _*).select(struct(col("*")) as "specimen")
  }

  private val specimenMappings: List[Transformation] = List(
    Custom { input =>
      val specimen = input
        .select("fhir_id", "release_id", "study_id", "type", "identifier", "collection", "subject", "status", "container", "parent", "processing", "meta")
        .withColumn("sample_type", firstSystemEquals(col("type")("coding"), SYS_SAMPLE_TYPE)("display"))
        .withColumn("sample_id", officialIdentifier)
        .withColumn("laboratory_procedure", col("processing")(0)("description"))
        .withColumn("participant_fhir_id", extractReferenceId(col("subject")("reference")))
        .withColumn("age_at_biospecimen_collection", col("collection._collectedDateTime.relativeDateTime.offset.value"))
        .withColumn("age_at_biospecimen_collection_years", floor(col("age_at_biospecimen_collection") / 365).cast("int"))
        .withColumn("age_at_biospecimen_collection_onset", age_on_set(col("age_at_biospecimen_collection_years"), age_at_bio_collection_on_set_intervals))
        .withColumn("container", explode_outer(col("container")))
        .withColumn("container_id", col("container")("identifier")(0)("value"))
        .withColumn("volume", col("container")("specimenQuantity")("value"))
        .withColumn("volume_unit", col("container")("specimenQuantity")("unit"))
        .withColumn("biospecimen_storage", col("container")("description"))
        .withColumn("parent", col("parent")(0))
        .withColumn("parent_id", extractReferenceId(col("parent.reference")))
        .withColumn("parent_0", struct(col("fhir_id"), col("sample_id"), col("parent_id"), col("sample_type"), lit(0) as "level"))
        .withColumn("external_sample_id", extractSecondaryIdentifier(col("identifier"), "/specimen"))
        .withColumn("method_of_sample_procurement", col("collection.method.text"))
        .withColumn("ncit_anatomy_site_id", extractNcitAnatomySiteId(col("collection.bodySite.coding")))
        .withColumn("anatomy_site", col("collection.bodySite.text"))
        .withColumn("tissue_type_source_text", col("type")("text"))
        .withColumn("ncit_id_tissue_type", extractNcitAnatomySiteId(col("type")("coding")))
        .withColumn("consent_type", extractConsentType("consent_type"))
        .withColumn("dbgap_consent_code", extractConsentType("dbgap_consent_code"))

      val grouped = addParentsToSpecimen(specimen)
        .groupBy("specimen.fhir_id", "specimen.container_id")
        .agg(first("specimen") as "specimen")
        .select("specimen.*")
      grouped

    },
    Drop(
      "type",
      "identifier",
      "collection",
      "subject",
      "parent",
      "container",
      "collection_sample",
      "meta",
      "processing"
    )
  )

  def apply(): List[Transformation] = specimenMappings
}
