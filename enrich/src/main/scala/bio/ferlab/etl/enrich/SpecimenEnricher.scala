package bio.ferlab.etl.enrich

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.time.LocalDateTime

/**
 * This step enrich specimen in order to join with occurrences and variant tables.
 * @param studyIds
 * @param configuration
 */
class SpecimenEnricher(studyIds: List[String])(implicit configuration: Configuration) extends ETLSingleDestination {
  override val mainDestination: DatasetConf = conf.getDataset("enriched_specimen")
  private val patient: DatasetConf = conf.getDataset("normalized_patient")
  private val family: DatasetConf = conf.getDataset("normalized_group")
  private val family_relationship: DatasetConf = conf.getDataset("normalized_family_relationship")
  private val proband_observation: DatasetConf = conf.getDataset("normalized_proband_observation")
  private val specimen: DatasetConf = conf.getDataset("normalized_specimen")
  private val disease: DatasetConf = conf.getDataset("normalized_disease")

  override def extract(lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    Seq(patient, family, family_relationship, specimen, disease, proband_observation)
      .map(ds => ds.id -> ds.read
        .where(col("study_id").isin(studyIds: _*))
      ).toMap
  }

  override def transformSingle(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val participants = data(patient.id)
      .select($"fhir_id" as "participant_fhir_id", $"participant_id", $"gender")
      .join(data(proband_observation.id).select("participant_fhir_id", "is_proband"), Seq("participant_fhir_id"), "left_outer")
      .withColumn("is_proband", coalesce(col("is_proband"), lit(false)))

    val familyDF = data(family.id).select(col("family_members_id"), col("fhir_id") as "family_fhir_id", col("family_id"))
    val affectedStatus = data(disease.id).select("participant_fhir_id", "affected_status").where($"affected_status").groupBy("participant_fhir_id").agg(first("affected_status") as "affected_status")

    val relations = data(family_relationship.id)
      .select($"participant2_fhir_id" as "participant_fhir_id", $"participant1_fhir_id" as "related_participant_id", $"participant1_to_participant_2_relationship" as "relation")
      .where($"relation" isin("father", "mother"))
      .groupBy("participant_fhir_id")
      .agg(collect_list(struct($"related_participant_id", $"relation")) as "relations")
      .withColumn("family", struct(
        functions.filter(col("relations"), c => c("relation") === "father")(0)("related_participant_id") as "father_id",
        functions.filter(col("relations"), c => c("relation") === "mother")(0)("related_participant_id") as "mother_id"
      ))
      .drop("relations")

    data(specimen.id).select($"sample_id", $"fhir_id" as "sample_fhir_id", $"participant_fhir_id", $"consent_code")
      .join(participants, Seq("participant_fhir_id"))
      .join(familyDF, array_contains(col("family_members_id"), col("participant_fhir_id")))
      .drop("family_members_id")
      .join(relations, Seq("participant_fhir_id"), "left")
      .join(affectedStatus, Seq("participant_fhir_id"), "left")
      .withColumn("affected_status", coalesce($"affected_status", lit(false)))
  }
}