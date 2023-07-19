package bio.ferlab.etl.enrich

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.functions.{array, array_contains, array_union, coalesce, col, collect_list, collect_set, explode, first, lit, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import scala.collection.immutable.Seq

class FamilyEnricher(studyIds: List[String])(implicit configuration: Configuration) extends ETLSingleDestination {
  override val mainDestination: DatasetConf = conf.getDataset("enriched_family")
  private val normalized_proband_observation: DatasetConf = conf.getDataset("normalized_proband_observation")
  private val normalized_patient: DatasetConf = conf.getDataset("normalized_patient")
  private val normalized_group: DatasetConf = conf.getDataset("normalized_group")
  private val normalized_family_relationship: DatasetConf = conf.getDataset("normalized_family_relationship")

  override def extract(lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    //FIXME duplicate accross project
    Seq(normalized_proband_observation, normalized_patient, normalized_group, normalized_family_relationship)
      .map(ds => ds.id -> ds.read
        .where(col("study_id").isin(studyIds: _*))
      ).toMap
  }

  override def transformSingle(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    val patients = data(normalized_patient.id)
    val probandObservations = data(normalized_proband_observation.id)
    val groups = data(normalized_group.id)
    val familyRelationships = data(normalized_family_relationship.id)


    val probands = patients
      .select(
        col("fhir_id") as "participant_fhir_id",
        col("participant_id")
      )
      .join(probandObservations.select("participant_fhir_id", "is_proband"), Seq("participant_fhir_id"), "left_outer")
      .withColumn("is_proband", coalesce(col("is_proband"), lit(false)))
      .where(col("is_proband"))
      .select(
        "participant_fhir_id",
        "participant_id"
      )


    val familiesWithProband = groups
      .join(probands, array_contains(groups("family_members_id"), probands("participant_fhir_id")), "inner")
      .select(
        probands("participant_fhir_id") as "participant_fhir_id",
        probands("participant_id") as "participant_id",
        groups("fhir_id") as "family_fhir_id"
      )

    val fr = familyRelationships
      .select(
        col("participant2_fhir_id") as "pt_fhir_id_with_focus",
        col("participant1_fhir_id") as "pt_fhir_id_of_subject",
        col("participant1_to_participant_2_relationship") as "role_of_subject_from_focus_view"
      )

    val probandJoinedFamilyJoinedRelation = fr
      .join(familiesWithProband, fr("pt_fhir_id_with_focus") === familiesWithProband("participant_fhir_id"))
      .drop("participant_fhir_id")

    val relationsFromProbandViewByFamily = probandJoinedFamilyJoinedRelation
      .groupBy("family_fhir_id")
      .agg(
        collect_set(col("participant_id"))(0) as "proband_participant_id",
        array_union(
          collect_list(
            struct(
              col("pt_fhir_id_of_subject") as "fhir_id",
              col("role_of_subject_from_focus_view") as "role"
            )
          ),
          array(
            struct(
              first(col("pt_fhir_id_with_focus")) as "fhir_id",
              lit("proband") as "role"
            )
          )
        ) as "relations",
      )


    groups
      .select(
        explode(col("family_members_id")) as "participant_fhir_id",
        col("fhir_id") as "family_fhir_id",
      )
      .join(relationsFromProbandViewByFamily, Seq("family_fhir_id"))
  }
}