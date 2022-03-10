package bio.ferlab.fhir.etl.common

import bio.ferlab.fhir.etl.common.OntologyUtils._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object Utils {

  val observableTitleStandard: Column => Column = term => trim(regexp_replace(term, "_", ":"))

  val getFamilyType: UserDefinedFunction =
    udf((arr: Seq[(String, String)], members: Seq[String]) => arr.map(_._2) match {
      case l if l.contains("mother") && l.contains("father") => if (members.length > 3) "trio+" else "trio"
      case l if l.contains("mother") || l.contains("father") => if (members.length > 2) "duo+" else "duo"
      case l if l.isEmpty => "proband-only"
      case _ => "other"
    })

  val downsyndromeStatusExtract: Column => Column = diagnoses => when(diagnoses.isNotNull && exists(diagnoses, d => trim(lower(d)) like "%down syndrome%"), "T21").otherwise("Other")

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

      val diseases = addDiseases(diagnosesDF)
      val commonColumns = Seq("participant_fhir_id", "study_id")

      val diseaseColumns = diseases.columns.filter(col => !commonColumns.contains(col))

      val diseasesWithMondoTerms =
        mapObservableTerms(diseases, "mondo_id_diagnosis")(mondoTerms)
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
          )

      val diseasesExplodedWithMondoTerms = diseasesWithMondoTerms
        .withColumn("mondo", explode(col("mondo")))

      val diseasesWithMondoTermsGrouped = {
        groupObservableTermsByAge(diseasesExplodedWithMondoTerms, "mondo")
      }

      val diseasesWithReplacedMondoTerms =
        diseasesWithMondoTerms
          .drop("mondo")
          .join(diseasesWithMondoTermsGrouped, Seq("participant_fhir_id"), "left_outer")

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
          .withColumn("participant_fhir_id", explode_outer(col("participant_fhir_ids")))
          .withColumn("specimen_fhir_id_file", explode_outer(col("specimen_fhir_ids")))
          .join(biospecimenDfReformat,
            col("specimen_fhir_id_file") === biospecimenDfReformat("specimen_fhir_id") &&
              biospecimenDfReformat("specimen_participant_fhir_id") === col("participant_fhir_id"),
            "full")
          .withColumn("file_name", when(col("fhir_id").isNull, "dummy_file").otherwise(col("file_name")))
          .withColumn("participant_fhir_id",
            when(col("fhir_id").isNull, col("biospecimen.participant_fhir_id"))
              .otherwise(col("participant_fhir_id"))
          )
          .groupBy("fhir_id", "participant_fhir_id")
          .agg(collect_list(col("biospecimen")) as "biospecimens", filesWithSeqExpDF.columns.filter(!_.equals("fhir_id")).map(c => first(c).as(c)): _*)

      val filesWithBiospecimenGroupedByParticipantIdDf =
        filesWithBiospecimenDf
          .withColumn("file", struct(filesWithBiospecimenDf.columns.filterNot(c => c.equals("participant_fhir_id")).map(col): _*))
          .select("participant_fhir_id", "file")
          .groupBy("participant_fhir_id")
          .agg(collect_list(col("file")) as "files")

      df
        .join(filesWithBiospecimenGroupedByParticipantIdDf, df("fhir_id") === filesWithBiospecimenGroupedByParticipantIdDf("participant_fhir_id"), "left_outer")
        .withColumn("files", coalesce(col("files"), array()))
        .drop("participant_fhir_id")
    }

    def addFileParticipantsWithBiospecimen(participantDf: DataFrame, biospecimensDf: DataFrame): DataFrame = {

      val biospecimensDfReformat = reformatBiospecimen(biospecimensDf)

      def buildMappingTable(): DataFrame = {
        // Link file - biospecimen
        val fileIdBiospecimenId = df
          .withColumn("specimen_fhir_id", explode(col("specimen_fhir_ids")))
          .withColumnRenamed("fhir_id", "file_fhir_id")
          .select("file_fhir_id", "specimen_fhir_id")

        // Link file - biospecimen - participant
        val fileIdBiospecimenIdParticipantId = fileIdBiospecimenId
          .join(biospecimensDf, fileIdBiospecimenId("specimen_fhir_id") === biospecimensDf("fhir_id"), "left_outer")
          .select("file_fhir_id", "specimen_fhir_id", "participant_fhir_id")

        // Link file - participant (useful for file without biospecimen)
        val fileIdParticipantId = df
          .withColumn("participant_fhir_id", explode(col("participant_fhir_ids")))
          .withColumnRenamed("fhir_id", "file_fhir_id")
          .select("file_fhir_id", "participant_fhir_id")

        // Mapping table with: file - (biospecimen) - participant
        fileIdParticipantId
          .join(fileIdBiospecimenIdParticipantId, Seq("file_fhir_id", "participant_fhir_id"), "left_outer")
      }

      // Mapping table with: file - (biospecimen) - participant
      val mappingTable = buildMappingTable()

      // |file_fhir_id|participant_fhir_id|biospecimens|
      val mappingTableWithBiospecimens = mappingTable
        .join(biospecimensDfReformat, mappingTable("specimen_fhir_id") === biospecimensDfReformat("specimen_fhir_id"), "left_outer")
        .drop("specimen_fhir_ids", "specimen_fhir_id")
        .groupBy("file_fhir_id", "participant_fhir_id")
        .agg(collect_list(col("biospecimen")) as "biospecimens")

      // |file_fhir_id|participants|
      val mappingTableWithParticipants = mappingTableWithBiospecimens
        .join(participantDf, mappingTableWithBiospecimens("participant_fhir_id") === participantDf("fhir_id"))
        .withColumn("participant", struct((participantDf.columns :+ "biospecimens").map(col): _*))
        .select("file_fhir_id", "participant")
        .groupBy("file_fhir_id")
        .agg(collect_list(col("participant")) as "participants")

      reformatSequencingExperiment(df)
        .join(mappingTableWithParticipants, df("fhir_id") === mappingTableWithParticipants("file_fhir_id"))
        .drop("file_fhir_id", "document_reference_fhir_id")
    }

    def addBiospecimenFiles(filesDf: DataFrame): DataFrame = {
      val filesWithSeqExperiments = reformatSequencingExperiment(filesDf)
      val reformatFile = filesWithSeqExperiments
        .withColumn("biospecimen_fhir_id", explode(col("specimen_fhir_ids")))
        .drop("document_reference_fhir_id")
        .withColumn("file", struct((filesWithSeqExperiments.columns).map(col): _*))
        .select("biospecimen_fhir_id", "file")
        .groupBy("biospecimen_fhir_id")
        .agg(collect_list(col("file")) as "files")

      df
        .join(reformatFile, df("fhir_id") === reformatFile("biospecimen_fhir_id"), "left_outer")
        .withColumn("files", coalesce(col("files"), array()))
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
