package bio.ferlab.etl.enriched.clinical

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDateTime
import scala.collection.immutable.Seq

case class FamilyEnricher(rc: RuntimeETLContext, studyIds: List[String]) extends SimpleSingleETL(rc) {
  override val mainDestination: DatasetConf = conf.getDataset("enriched_family")
  private val normalized_proband_observation: DatasetConf = conf.getDataset("normalized_proband_observation")
  private val normalized_patient: DatasetConf = conf.getDataset("normalized_patient")
  private val normalized_group: DatasetConf = conf.getDataset("normalized_group")
  private val normalized_family_relationship: DatasetConf = conf.getDataset("normalized_family_relationship")

  override def extract(lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime): Map[String, DataFrame] = {
    //FIXME duplicate accross project
    Seq(normalized_proband_observation, normalized_patient, normalized_group, normalized_family_relationship)
      .map(ds => ds.id -> ds.read
        .where(col("study_id").isin(studyIds: _*))
      ).toMap
  }

  override def transformSingle(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime): DataFrame = {
    /**
     * We find all families where one of its members is a proband.
     * A relation mapping for every one of these particular families is built.
     * More precisely, we have the role of every member from the point of view of the proband.
     * Given the family, {p, pf, pm} with get:
     * [{p, role: proband}, {pf, role: father}, {pm, role: mother}]
     * Therefore, the output may look like (pls make sure that this comment is up-to-date!)
     *
     * +--------------+------------+-----------------------------------+------------+
     * |participant_id|families_id|family                              |family_type |
     * +--------------+-----------+------------------------------------+------------+
     * |p            |f           |{[{p, proband}, {pm, mother}], f}   |duo         |
     * |pm           |f           |{[{p, proband}, {pm, mother}], f}   |duo         |
     * +--------------+------------+-----------------------------------+------------+
     * */
    val patients = data(normalized_patient.id)
    val probandObservations = data(normalized_proband_observation.id)
    val groups = data(normalized_group.id)
    val familyRelationships = data(normalized_family_relationship.id)

    val probands = patients
      .select(
        col("fhir_id") as "participant_fhir_id",
        col("participant_id")
      )
      .join(
        // Possible code repeat across different modules.
        probandObservations.select("participant_fhir_id", "is_proband"),
        Seq("participant_fhir_id"),
        "left_outer")
      .withColumn("is_proband", coalesce(col("is_proband"), lit(false)))
      .where(col("is_proband"))
      .select(
        "participant_fhir_id",
        "participant_id"
      )

    val familiesWithProband = groups
      .join(probands, array_contains(groups("family_members_id"), probands("participant_fhir_id")), "inner")
      .select(
        probands("participant_fhir_id") as "proband_fhir_id",
        groups("fhir_id") as "family_fhir_id"
      )

    val patientsFhirIdToParticipantId = patients
      .select(
        col("fhir_id") as "fhir_id_from_pt",
        col("participant_id")
      )

    val fr = familyRelationships
      .select(
        col("participant2_fhir_id") as "pt_fhir_id_with_focus",
        col("participant1_fhir_id") as "pt_fhir_id_of_subject",
        col("participant1_to_participant_2_relationship") as "role_of_subject_from_focus_view"
      )

    val frWithProbandId = fr
      .join(
        familiesWithProband,
        fr("pt_fhir_id_with_focus") === familiesWithProband("proband_fhir_id")
      )
      .drop("proband_fhir_id")

    val frWithProbandIdWithFocusId = frWithProbandId
      .join(
        patientsFhirIdToParticipantId,
        frWithProbandId("pt_fhir_id_with_focus") === patientsFhirIdToParticipantId("fhir_id_from_pt"),
        "left"
      )
      .withColumnRenamed("participant_id", "participant_focus_id")
      .drop(patientsFhirIdToParticipantId("fhir_id_from_pt"))

    val frWithProbandIdWithFocusIdWithSubjectId = frWithProbandIdWithFocusId
      .join(
        patientsFhirIdToParticipantId,
        frWithProbandId("pt_fhir_id_of_subject") === patientsFhirIdToParticipantId("fhir_id_from_pt"),
        "left"
      )
      .withColumnRenamed("participant_id", "participant_subject_id")
      .drop(patientsFhirIdToParticipantId("fhir_id_from_pt"))


    val relationsFromProbandViewByFamily = frWithProbandIdWithFocusIdWithSubjectId
      .groupBy("family_fhir_id")
      .agg(
        array_union(
          collect_list(
            struct(
              col("participant_subject_id") as "participant_id",
              col("role_of_subject_from_focus_view") as "role"
            )
          ),
          array(
            struct(
              first(col("participant_focus_id")) as "participant_id",
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