package bio.ferlab.etl.prepared.clinical

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class StudyCentric(rc: RuntimeETLContext, studyIds: List[String]) extends SimpleSingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_study_centric")
  private val normalized_researchstudy: DatasetConf = conf.getDataset("normalized_research_study")
  val normalized_drs_document_reference: DatasetConf = conf.getDataset("normalized_document_reference")
  val normalized_patient: DatasetConf = conf.getDataset("normalized_patient")
  val normalized_group: DatasetConf = conf.getDataset("normalized_group")
  val normalized_specimen: DatasetConf = conf.getDataset("normalized_specimen")
  val normalized_sequencing_experiment: DatasetConf = conf.getDataset("normalized_sequencing_experiment")

  override def extract(lastRunDateTime: LocalDateTime = minValue,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Seq(
      normalized_researchstudy,
      normalized_drs_document_reference,
      normalized_patient,
      normalized_group,
      normalized_specimen,
      normalized_sequencing_experiment
    )
      .map(ds => ds.id -> ds.read
        .where(col("study_id").isin(studyIds: _*))
      ).toMap

  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minValue,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    val studyDF = data(normalized_researchstudy.id)

    val countPatientDf = data(normalized_patient.id).groupBy("study_id").count().withColumnRenamed("count", "participant_count")
    val countFamilyDf = data(normalized_group.id).filter(size(col("family_members")).gt(1)).groupBy("study_id").count().withColumnRenamed("count", "family_count")

    val countFileDf = data(normalized_drs_document_reference.id)
      .groupBy("study_id")
      .agg(count(lit(1)) as "file_count",
        collect_set(col("experiment_strategy")) as "experimental_strategy_file",
        collect_set(col("data_category")) as "data_category",
        collect_set(col("controlled_access")) as "controlled_access"
      )

    val countBiospecimenDf = data(normalized_specimen.id)
      .groupBy("study_id")
      .agg(
        count(lit(1)) as "biospecimen_count",
      )

    val aggSeqExpDf = data(normalized_sequencing_experiment.id)
      .groupBy("study_id")
      .agg(
        collect_set(col("experiment_strategy")) as "experimental_strategy_seq_exp"
      )
    val transformedStudyDf = studyDF
      .withColumnRenamed("name", "study_name")
      .join(countPatientDf, Seq("study_id"), "left_outer")
      .withColumn("participant_count", coalesce(col("participant_count"), lit(0)))
      .join(countFileDf, Seq("study_id"), "left_outer")
      .join(countBiospecimenDf, Seq("study_id"), "left_outer")
      .withColumn("file_count", coalesce(col("file_count"), lit(0)))
      .join(countFamilyDf, Seq("study_id"), "left_outer")
      .withColumn("family_count", coalesce(col("family_count"), lit(0)))
      .withColumn("family_data", col("family_count").gt(0))
      .withColumn("search_text", array(
        col("study_name"), col("study_code"), col("external_id")
      ))
      .withColumn("search_text", filter(col("search_text"), x => x.isNotNull && x =!= ""))
      .join(aggSeqExpDf, Seq("study_id"), "left_outer")
      .withColumn("experimental_strategy", array_union(col("experimental_strategy_seq_exp"), col("experimental_strategy_file")))
      .drop("experimental_strategy_seq_exp", "experimental_strategy_file")

    transformedStudyDf
  }


}
