package bio.ferlab.fhir.etl.common

import bio.ferlab.fhir.etl.common.OntologyUtils._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

object Utils {
  val DOWN_SYNDROM_MONDO_TERM = "MONDO:0008608"

  val observableTitleStandard: Column => Column = term => trim(regexp_replace(term, "_", ":"))

  val getFamilyType: UserDefinedFunction =
    udf((arr: Seq[(String, String)], members: Seq[String]) => arr.map(_._2) match {
      case l if l.contains("mother") && l.contains("father") => if (members.length > 3) "trio+" else "trio"
      case l if l.contains("mother") || l.contains("father") => if (members.length > 2) "duo+" else "duo"
      case l if l.isEmpty => "proband-only"
      case _ => "other"
    })

  val sequencingExperimentCols = Seq("fhir_id", "sequencing_experiment_id", "experiment_strategy",
    "instrument_model", "library_name", "library_strand", "platform")

  private def reformatSequencingExperiment(documentDF: DataFrame) = {
    documentDF.withColumn("sequencing_experiment", struct(col("experiment_strategy")))
      .drop("experiment_strategy")
  }

  private def reformatBiospecimen(biospecimensDf: DataFrame) = {
    biospecimensDf
      .withColumn("biospecimen", struct(biospecimensDf.columns.map(col): _*))
      .withColumnRenamed("fhir_id", "specimen_fhir_id")
      .withColumnRenamed("participant_fhir_id", "specimen_participant_fhir_id")
      .select("specimen_fhir_id", "specimen_participant_fhir_id", "biospecimen")
  }


  implicit class DataFrameOperations(df: DataFrame) {
    def addStudy(studyDf: DataFrame): DataFrame = {
      val reformatStudy: DataFrame = studyDf
        .withColumn("study", struct(studyDf.columns.filter(!_.equals("release_id")).map(col): _*))
        .select("study_id", "study")

      df.join(reformatStudy, "study_id")
    }

    def addOutcomes(vitalStatusDf: DataFrame): DataFrame = {
      val reformatObservation: DataFrame = vitalStatusDf
        .withColumn("outcome", struct(vitalStatusDf.columns.map(col): _*))
        .select("participant_fhir_id", "outcome")
        .groupBy("participant_fhir_id")
        .agg(
          collect_list(col("outcome")) as "outcomes"
        )

      df
        .join(reformatObservation, col("fhir_id") === col("participant_fhir_id"), "left_outer")
        .withColumn("outcomes", coalesce(col("outcomes"), array()))
        .drop("participant_fhir_id")
    }

    def addDownSyndromeDiagnosis(diseases: DataFrame, mondoTerms: DataFrame): DataFrame = {
      val mondoDownSyndrome = mondoTerms.where(
        exists(col("parents"), p=> p like s"%$DOWN_SYNDROM_MONDO_TERM%") || col("id") === DOWN_SYNDROM_MONDO_TERM).select(col("id") as "mondo_down_syndrome_id", col("name") as "mondo_down_syndrome_name")

      val downSyndromeDiagnosis = diseases.join(mondoDownSyndrome, col("mondo_id") === col("mondo_down_syndrome_id"))
        .select(
          col("participant_fhir_id"),
          when(col("mondo_down_syndrome_id").isNotNull, displayTerm(col("mondo_down_syndrome_id"), col("mondo_down_syndrome_name")))
            .otherwise(null) as "down_syndrome_diagnosis"
        )
        .groupBy("participant_fhir_id")
        .agg(collect_set("down_syndrome_diagnosis") as "down_syndrome_diagnosis")
      df.join(downSyndromeDiagnosis, col("fhir_id") === col("participant_fhir_id"), "left_outer")
        .withColumn("down_syndrome_status", when(size(col("down_syndrome_diagnosis")) > 0, "T21").otherwise("Other"))
        .drop("participant_fhir_id")

    }

    def addDiagnosisPhenotypes(phenotypeDF: DataFrame, diagnosesDF: DataFrame)(hpoTerms: DataFrame, mondoTerms: DataFrame): DataFrame = {
      val phenotypes = addPhenotypes(phenotypeDF)
      val phenotypesWithHPOTerms =
        mapObservableTerms(phenotypes, "observable_term")(hpoTerms)
          .groupBy("participant_fhir_id")
          .agg(
            collect_list(struct(
              col("fhir_id"),
              col("hpo_phenotype_observed"),
              col("hpo_phenotype_not_observed"),
              col("age_at_event_days"),
              col("is_observed")
            )) as "phenotype",
            collect_list(
              when(col("is_observed"), col("observable_with_ancestors"))
            ) as "observed_phenotype",
            collect_list(
              when(not(col("is_observed")), col("observable_with_ancestors"))
            ) as "non_observed_phenotype"
          )

      val phenotypesWithHPOTermsObsExploded =
        phenotypesWithHPOTerms
          .withColumn(s"observed_phenotype_exp", explode(col("observed_phenotype")))
          .withColumn("observed_phenotype", explode(col("observed_phenotype_exp")))

      val phenotypesWithHPOTermsNonObsExploded =
        phenotypesWithHPOTerms
          .withColumn(s"non_observed_phenotype_exp", explode(col("non_observed_phenotype")))
          .withColumn("non_observed_phenotype", explode(col("non_observed_phenotype_exp")))


      val observedPhenotypes = groupObservableTermsByAge(phenotypesWithHPOTermsObsExploded, "observed_phenotype")
      val nonObservedPhenotypes = groupObservableTermsByAge(phenotypesWithHPOTermsNonObsExploded, "non_observed_phenotype")

      val phenotypesWithHPOTermsGroupedByEvent =
        phenotypesWithHPOTerms
          .drop("observed_phenotype", "non_observed_phenotype")
          .join(observedPhenotypes, Seq("participant_fhir_id"), "left_outer")
          .join(nonObservedPhenotypes, Seq("participant_fhir_id"), "left_outer")

      val diseases = addDiseases(diagnosesDF, mondoTerms)
      val commonColumns = Seq("participant_fhir_id", "study_id")

      val diseaseColumns = diseases.columns.filter(col => !commonColumns.contains(col))

      val diseasesWithMondoTerms =
        mapObservableTerms(diseases, "mondo_id")(mondoTerms)
          .withColumn("mondo", explode_outer(col("observable_with_ancestors")))
          .drop("observable_with_ancestors", "study_id")
          .groupBy("participant_fhir_id")
          .agg(
            collect_set(
              struct(
                diseaseColumns.head,
                diseaseColumns.tail: _*
              )
            ) as "diagnosis",
            collect_set(col("mondo")) as "mondo"
          ).drop("mondo_id")

      val diseasesExplodedWithMondoTerms = diseasesWithMondoTerms
        .withColumn("mondo", explode(col("mondo")))

      val diseasesWithMondoTermsGrouped = {
        groupObservableTermsByAge(diseasesExplodedWithMondoTerms, "mondo")
      }

      val diseasesWithReplacedMondoTerms =
        diseasesWithMondoTerms
          .drop("mondo")
          .join(diseasesWithMondoTermsGrouped, Seq("participant_fhir_id"), "left_outer")
          .drop("mondo_down_syndrome_id", "mondo_down_syndrome_name", "mondo_id")

      df
        .join(phenotypesWithHPOTermsGroupedByEvent, col("fhir_id") === col("participant_fhir_id"), "left_outer")
        .drop("participant_fhir_id")
        .join(diseasesWithReplacedMondoTerms, col("fhir_id") === col("participant_fhir_id"), "left_outer")
        .drop("participant_fhir_id")

    }

    def addParticipantFilesWithBiospecimen(filesDf: DataFrame, biospecimensDf: DataFrame): DataFrame = {

      val biospecimenDfReformat = reformatBiospecimen(biospecimensDf)

      val filesWithSeqExpDF = reformatSequencingExperiment(filesDf)

      val filesWithBiospecimenDf =
        filesWithSeqExpDF
          .withColumn("specimen_fhir_id_file", explode_outer(col("specimen_fhir_ids")))
          .join(biospecimenDfReformat,
            col("specimen_fhir_id_file") === biospecimenDfReformat("specimen_fhir_id"),
            "full")
          .withColumnRenamed("participant_fhir_id", "participant_fhir_id_file")
          .withColumn("file_name", when(col("fhir_id").isNull, "dummy_file").otherwise(col("file_name")))
          .withColumn("participant_fhir_id",
            when(col("biospecimen.participant_fhir_id").isNotNull, col("biospecimen.participant_fhir_id"))
              .otherwise(col("participant_fhir_id_file"))
          )
          .drop("participant_fhir_id_file")
          .groupBy("fhir_id", "participant_fhir_id")
          .agg(collect_list(col("biospecimen")) as "biospecimens",
            filesWithSeqExpDF.columns.filter(c => !c.equals("fhir_id") && !c.equals("participant_fhir_id")).map(c => first(c).as(c)): _*)
          .drop("specimen_fhir_ids")

      val filesWithBiospecimenGroupedByParticipantIdDf =
        filesWithBiospecimenDf
          .withColumn("file", struct(filesWithBiospecimenDf.columns.filterNot(c => c.equals("participant_fhir_id")).map(col): _*))
          .select("participant_fhir_id", "file")
          .groupBy("participant_fhir_id")
          .agg(
            count(col("file.file_id")) as "nb_files",
            collect_list(col("file")) as "files",
            size(flatten(collect_set(col("file.biospecimens.fhir_id")))) as "nb_biospecimens"
          )

      df
        .join(filesWithBiospecimenGroupedByParticipantIdDf, df("fhir_id") === filesWithBiospecimenGroupedByParticipantIdDf("participant_fhir_id"), "left_outer")
        .withColumn("files", coalesce(col("files"), array()))
        .drop("participant_fhir_id")
    }

    def addFileParticipantsWithBiospecimen(participantDf: DataFrame, biospecimensDf: DataFrame): DataFrame = {

      val fileWithSeqExp = reformatSequencingExperiment(df)
        .withColumn("specimen_fhir_id", explode_outer(col("specimen_fhir_ids")))

      val biospecimensDfReformat = reformatBiospecimen(biospecimensDf)

      val fileWithBiospecimen = fileWithSeqExp
        .select(struct(col("*")) as "file")
        .join(biospecimensDfReformat, col("file.specimen_fhir_id") === biospecimensDfReformat("specimen_fhir_id"), "left_outer")
        .withColumn("participant_file_fhir_id", when(biospecimensDfReformat("specimen_participant_fhir_id").isNotNull, biospecimensDfReformat("specimen_participant_fhir_id")).otherwise(col("file.participant_fhir_id")))
        .groupBy("file.fhir_id", "participant_file_fhir_id")
        .agg(collect_list(col("biospecimen")) as "biospecimens", first("file") as "file", count(col("biospecimen.fhir_id")) as "nb_biospecimens")

      val participantReformat = participantDf.select(struct(col("*")) as "participant")
      fileWithBiospecimen
        .join(participantReformat, col("participant_file_fhir_id") === col("participant.fhir_id"))
        .withColumn("participant", struct(col("participant.*"), col("biospecimens")))
        .drop("biospecimens")
        .groupBy(col("fhir_id"))
        .agg(collect_list(col("participant")) as "participants", first("file") as "file", count(lit(1)) as "nb_participants", sum("nb_biospecimens") as "nb_biospecimens")
        .select(col("file.*"), col("participants"), col("nb_participants"), col("nb_biospecimens"))


    }

    def addBiospecimenFiles(filesDf: DataFrame): DataFrame = {
      val filesWithSeqExperiments = reformatSequencingExperiment(filesDf)
      val fileColumns = filesWithSeqExperiments.columns.collect { case c if c != "specimen_fhir_ids" => col(c) }
      val reformatFile = filesWithSeqExperiments
        .withColumn("biospecimen_fhir_id", explode(col("specimen_fhir_ids")))
        .drop("document_reference_fhir_id")
        .withColumn("file", struct(fileColumns: _*))
        .select("biospecimen_fhir_id", "file")
        .groupBy("biospecimen_fhir_id")
        .agg(collect_list(col("file")) as "files")

      df
        .join(reformatFile, df("fhir_id") === reformatFile("biospecimen_fhir_id"), "left_outer")
        .withColumn("files", coalesce(col("files"), array()))
        .withColumn("nb_files", size(col("files")))
        .drop("biospecimen_fhir_id")
    }

    def addBiospecimenParticipant(participantsDf: DataFrame): DataFrame = {
      val reformatParticipant: DataFrame = participantsDf
        .withColumn("participant", struct(participantsDf.columns.map(col): _*))
        .withColumnRenamed("fhir_id", "participant_fhir_id")
        .select("participant_fhir_id", "participant")

      df.join(reformatParticipant, "participant_fhir_id")
    }

    def addFamily(familyDf: DataFrame, familyRelationshipDf: DataFrame): DataFrame = {
      val familyRelationshipCols = Seq("family_id", "type", "family_members", "family_members_id")

      val cleanFamilyRelationshipDf = familyRelationshipDf
        .drop("study_id", "release_id", "fhir_id")

      val reformatFamily = familyDf
        .withColumn(s"family_members_id_exp", explode(col("family_members_id")))
        .join(cleanFamilyRelationshipDf, col("participant1_fhir_id") === col("family_members_id_exp"), "left_outer")
        .drop("observation_id", "participant1_fhir_id", "fam_relationship_fhir_id", "study_id", "release_id", "external_id")
        .withColumnRenamed("family_members_id_exp", "participant1_fhir_id")
        .withColumnRenamed("fhir_id", "family_fhir_id")

      val cols = df.columns ++ familyRelationshipCols :+ "family_fhir_id"

      df.join(reformatFamily, col("fhir_id") === col("participant2_fhir_id"), "left_outer")
        .drop("participant2_fhir_id")
        .groupBy(cols.map(col): _*)
        .agg(
          collect_list(
            when(col("participant1_fhir_id").isNotNull,
              struct(
                col("participant1_fhir_id") as "related_participant_id",
                col("participant1_to_participant_2_relationship") as "relation"
              )
            )
          ) as "family_relations"
        )
        .withColumn("family", when(col("family_fhir_id").isNotNull,
          struct(
            col("family_fhir_id") as "fhir_id",
            col("family_id"),
            col("type"),
            col("family_members_id"),
            col("family_relations")
          )
        ))
        .withColumn("family_type", getFamilyType(col("family_relations"), col("family_members_id")))
        .drop(familyRelationshipCols :+ "family_relations" :+ "family_fhir_id": _*)
    }
  }
}
