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
    val patients = data(normalized_patient.id).select(col("fhir_id") as "participant_fhir_id", col("participant_id"))
    val probandObservations = data(normalized_proband_observation.id)
    val groups = data(normalized_group.id)
    val familyRelationships = data(normalized_family_relationship.id)
      .select(
        col("participant2_fhir_id") as "focus",
        col("participant1_fhir_id") as "target",
        col("participant1_to_participant_2_relationship") as "role"
      )


    val probands = probandObservations
      .where(col("is_proband"))
      .join(patients, Seq("participant_fhir_id"))
      .select("participant_id", "participant_fhir_id", "study_id")

    val probandMember = array(struct(col("proband_id") as "participant_id", lit("proband") as "role"))

    val probandFamilyRelationShips = familyRelationships.join(probands, probands("participant_fhir_id") === familyRelationships("focus"))
      .select(col("participant_id") as "proband_id", col("study_id"), col("target"), col("role"))
      .join(patients.as("p"), col("target") === patients("participant_fhir_id"))
      .select(col("proband_id"), col("p.participant_id") as "participant_id_target", col("study_id"), col("role"))
      .groupBy(col("proband_id"), col("study_id"))
      .agg(collect_list(struct(col("participant_id_target") as  "participant_id", col("role"))) as "relations")
      .withColumn("relations", array_union(col("relations"), probandMember))//add proband to the list of relations

    val families = groups
      .join(probands, array_contains(groups("family_members_id"), probands("participant_fhir_id")), "inner")
      .select(
        probands("participant_id") as "proband_id",
        groups("fhir_id") as "family_fhir_id",
        explode(groups("family_members_id")) as "participant_fhir_id"
      )
      .join(patients, Seq("participant_fhir_id"))


    probandFamilyRelationShips.join(families, Seq("proband_id"))
      .drop("proband_id")

  }
}