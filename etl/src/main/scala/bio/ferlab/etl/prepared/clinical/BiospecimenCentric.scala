package bio.ferlab.etl.prepared.clinical

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.etl.prepared.clinical.Utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct}

import java.time.LocalDateTime

case class BiospecimenCentric(rc: RuntimeETLContext, studyIds: List[String]) extends SimpleSingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_biospecimen_centric")
  val normalized_specimen: DatasetConf = conf.getDataset("normalized_specimen")
  val normalized_drs_document_reference: DatasetConf = conf.getDataset("normalized_document_reference")
  val simple_participant: DatasetConf = conf.getDataset("simple_participant")
  val es_index_study_centric: DatasetConf = conf.getDataset("es_index_study_centric")
  val normalized_sequencing_experiment: DatasetConf = conf.getDataset("normalized_sequencing_experiment")
  val normalized_sequencing_experiment_genomic_file: DatasetConf = conf.getDataset("normalized_sequencing_experiment_genomic_file")
  private val enriched_histology_disease: DatasetConf = conf.getDataset("enriched_histology_disease")


  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {

    Seq(normalized_specimen, normalized_drs_document_reference, simple_participant, es_index_study_centric,
      normalized_sequencing_experiment, normalized_sequencing_experiment_genomic_file, enriched_histology_disease)
      .map(ds => ds.id -> ds.read
        .where(col("study_id").isin(studyIds: _*))
      ).toMap
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    val specimenDF = data(normalized_specimen.id)
    val filesWithSeqExp = data(normalized_drs_document_reference.id)
      .addSequencingExperiment(
        data(normalized_sequencing_experiment.id),
        data(normalized_sequencing_experiment_genomic_file.id)
      )
    val transformedBiospecimen =
      specimenDF
        .addStudy(data(es_index_study_centric.id))
        .addBiospecimenParticipant(data(simple_participant.id))
        .addBiospecimenFiles(filesWithSeqExp)
        .addHistologicalInformation(data(enriched_histology_disease.id))
        .withColumn("biospecimen_facet_ids", struct(col("fhir_id") as "biospecimen_fhir_id_1", col("fhir_id") as "biospecimen_fhir_id_2"))

    transformedBiospecimen
  }
}