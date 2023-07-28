package bio.ferlab.etl.prepared.clinical

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.etl.prepared.clinical.OntologyUtils.firstCategory
import bio.ferlab.etl.prepared.clinical.Utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, struct}

import java.time.LocalDateTime

case class SimpleParticipant(rc: RuntimeETLContext, studyIds: List[String]) extends SimpleSingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("simple_participant")
  val es_index_study_centric: DatasetConf = conf.getDataset("es_index_study_centric")
  val normalized_patient: DatasetConf = conf.getDataset("normalized_patient")
  val normalized_proband_observation: DatasetConf = conf.getDataset("normalized_proband_observation")
  val normalized_vital_status: DatasetConf = conf.getDataset("normalized_vital_status")
  val normalized_phenotype: DatasetConf = conf.getDataset("normalized_phenotype")
  val normalized_disease: DatasetConf = conf.getDataset("normalized_disease")
  val normalized_group: DatasetConf = conf.getDataset("normalized_group")
  val hpo_terms: DatasetConf = conf.getDataset("hpo_terms")
  val mondo_terms: DatasetConf = conf.getDataset("mondo_terms")
  val enriched_family: DatasetConf = conf.getDataset("enriched_family")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    (Seq(
      es_index_study_centric, normalized_patient, normalized_phenotype, normalized_disease, normalized_group, normalized_vital_status, normalized_proband_observation, enriched_family)
      .map(ds => ds.id -> ds.read
        .where(col("study_id").isin(studyIds: _*))
      ) ++ Seq(
      hpo_terms.id -> hpo_terms.read,
      mondo_terms.id -> mondo_terms.read,
    )).toMap

  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    val patientDF = data(normalized_patient.id)
    val disease = data(normalized_disease.id).withColumn("mondo_id", observableTitleStandard(firstCategory("MONDO", col("condition_coding"))))
    val transformedParticipant =
      patientDF
        .addStudy(data(es_index_study_centric.id))
        .addDiagnosisPhenotypes(
          data(normalized_phenotype.id),
          disease
        )(data(hpo_terms.id), data(mondo_terms.id))
        .addDownSyndromeDiagnosis(disease, data(mondo_terms.id))
        .addOutcomes(data(normalized_vital_status.id))
        .addProband(data(normalized_proband_observation.id))
        .addFamily(data(normalized_group.id), data(enriched_family.id))
        .withColumnRenamed("gender", "sex")
        .withColumn("age_at_data_collection", lit(111)) // TODO
        .withColumn("study_external_id", col("study")("external_id"))
        .withColumn("participant_facet_ids", struct(col("fhir_id") as "participant_fhir_id_1", col("fhir_id") as "participant_fhir_id_2"))

    transformedParticipant
  }
}
