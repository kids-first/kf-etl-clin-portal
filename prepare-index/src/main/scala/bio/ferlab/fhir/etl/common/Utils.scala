package bio.ferlab.fhir.etl.common

import bio.ferlab.fhir.etl.common.OntologyUtils._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{when, _}

object Utils {
  val DOWN_SYNDROM_MONDO_TERM = "MONDO:0008608"

  val observableTitleStandard: Column => Column = term => trim(regexp_replace(term, "_", ":"))

  private def reformatFileFacetIds(documentDF: DataFrame) = {
    documentDF
      .withColumn("file_facet_ids", struct(col("fhir_id") as "file_fhir_id_1", col("fhir_id") as "file_fhir_id_2"))
  }

  private def reformatBiospecimen(biospecimensDf: DataFrame) = {
    biospecimensDf
      .withColumn("biospecimen_facet_ids", struct(col("fhir_id") as "biospecimen_fhir_id_1", col("fhir_id") as "biospecimen_fhir_id_2"))
      .withColumn("biospecimen", struct((biospecimensDf.columns :+ "biospecimen_facet_ids").map(col): _*))
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

    def addProband(probandDF: DataFrame): DataFrame = {
      df
        .join(probandDF.select("participant_fhir_id", "is_proband"), col("fhir_id") === col("participant_fhir_id"), "left_outer")
        .withColumn("is_proband", coalesce(col("is_proband"), lit(false)))
        .drop("participant_fhir_id")
    }

    def addDownSyndromeDiagnosis(diseasesMondo: DataFrame, mondoTerms: DataFrame): DataFrame = {
      val mondoDownSyndrome = mondoTerms.where(
        exists(col("ancestors"), p => p("id") like s"%$DOWN_SYNDROM_MONDO_TERM%") || col("id") === DOWN_SYNDROM_MONDO_TERM).select(col("id") as "mondo_down_syndrome_id", col("name") as "mondo_down_syndrome_name")

      val downSyndromeDiagnosis = diseasesMondo.join(mondoDownSyndrome, col("mondo_id") === col("mondo_down_syndrome_id"))
        .select(
          col("participant_fhir_id"),
          when(col("mondo_down_syndrome_id").isNotNull, displayTerm(col("mondo_down_syndrome_id"), col("mondo_down_syndrome_name")))
            .otherwise(null) as "down_syndrome_diagnosis"
        )
        .groupBy("participant_fhir_id")
        .agg(collect_set("down_syndrome_diagnosis") as "down_syndrome_diagnosis")
      df.join(downSyndromeDiagnosis, col("fhir_id") === col("participant_fhir_id"), "left_outer")
        .withColumn("down_syndrome_status", when(size(col("down_syndrome_diagnosis")) > 0, "T21").otherwise("D21"))
        .drop("participant_fhir_id")

    }

    def addDiagnosisPhenotypes(phenotypeDF: DataFrame, diseasesMondoDF: DataFrame)(hpoTerms: DataFrame, mondoTerms: DataFrame): DataFrame = {
      val phenotypes = addPhenotypes(phenotypeDF, hpoTerms)

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

      val diseases = addDiseases(diseasesMondoDF, mondoTerms)
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

      val filesWithFacetIds = reformatFileFacetIds(filesDf)

      val filesWithBiospecimenDf =
        filesWithFacetIds
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
            filesWithFacetIds.columns.filter(c => !c.equals("fhir_id") && !c.equals("participant_fhir_id")).map(c => first(c).as(c)): _*)
          .withColumn("file_facet_ids", struct(col("fhir_id") as "file_fhir_id_1", col("fhir_id") as "file_fhir_id_2"))
          .drop("specimen_fhir_ids")

      val filesWithBiospecimenGroupedByParticipantIdDf =
        filesWithBiospecimenDf
          .withColumn("file", struct(filesWithBiospecimenDf.columns.filterNot(c => c.equals("participant_fhir_id")).map(col): _*))
          .withColumn("biospecimens_unique_ids", transform(col("file.biospecimens"), c => concat_ws("_", c("fhir_id"), c("container_id"))))
          .select("participant_fhir_id", "file", "biospecimens_unique_ids")
          .groupBy("participant_fhir_id")
          .agg(
            coalesce(count(col("file.file_id")), lit(0)) as "nb_files",
            collect_list(col("file")) as "files",
            coalesce(size(array_distinct(flatten(collect_set(col("biospecimens_unique_ids"))))), lit(0)) as "nb_biospecimens"
          )

      df
        .join(filesWithBiospecimenGroupedByParticipantIdDf, df("fhir_id") === filesWithBiospecimenGroupedByParticipantIdDf("participant_fhir_id"), "left_outer")
        .withColumn("files", coalesce(col("files"), array()))
        .drop("participant_fhir_id")
    }

    def addFileParticipantsWithBiospecimen(participantDf: DataFrame, biospecimensDf: DataFrame): DataFrame = {

      val fileWithSeqExp = reformatFileFacetIds(df)
        .withColumn("specimen_fhir_id", explode_outer(col("specimen_fhir_ids")))

      val biospecimensDfReformat = reformatBiospecimen(biospecimensDf)

      val fileWithBiospecimen = fileWithSeqExp
        .select(struct(col("*")) as "file")
        .join(biospecimensDfReformat, col("file.specimen_fhir_id") === biospecimensDfReformat("specimen_fhir_id"), "left_outer")
        .withColumn("participant_file_fhir_id", when(biospecimensDfReformat("specimen_participant_fhir_id").isNotNull, biospecimensDfReformat("specimen_participant_fhir_id")).otherwise(col("file.participant_fhir_id")))
        .withColumn("biospecimen_unique_id", when(col("biospecimen.fhir_id").isNotNull, concat_ws("_", col("biospecimen.fhir_id"), col("biospecimen.container_id"))).otherwise(null))
        .groupBy("file.fhir_id", "participant_file_fhir_id")
        .agg(collect_list(col("biospecimen")) as "biospecimens", first("file") as "file", count(col("biospecimen_unique_id")) as "nb_biospecimens")

      val participantReformat = participantDf.select(struct(col("*")) as "participant")
      fileWithBiospecimen
        .join(participantReformat, col("participant_file_fhir_id") === col("participant.fhir_id"))
        .withColumn("participant", struct(col("participant.*"), col("biospecimens")))
        .drop("biospecimens")
        .groupBy(col("fhir_id"))
        .agg(collect_list(col("participant")) as "participants", first("file") as "file", count(lit(1)) as "nb_participants", sum("nb_biospecimens") as "nb_biospecimens")
        .select(col("file.*"), col("participants"), col("nb_participants"), col("nb_biospecimens"))


    }

    def addSequencingExperiment(sequencingExperiment:DataFrame, sequencingExperimentGenomicFile:DataFrame): DataFrame = {
      val seqExpGenomicFileDF = sequencingExperimentGenomicFile
        .select(col("genomic_file") as "file_id", col("sequencing_experiment") as "sequencing_experiment_id")

      val seqExpDF = sequencingExperiment.select(
        col("kf_id") as "sequencing_experiment_id",
        struct(
          col("kf_id") as "sequencing_experiment_id",
          col("experiment_date"),
          col("experiment_strategy"),
          col("center"),
          col("library_name"),
          col("library_prep"),
          col("library_selection"),
          col("library_strand"),
          col("is_paired_end"),
          col("platform"),
          col("instrument_model"),
          col("max_insert_size"),
          col("mean_insert_size"),
          col("mean_depth"),
          col("total_reads"),
          col("mean_read_length"),
          col("external_id"),
          col("sequencing_center_id")
        ) as "sequencing_experiment"
      )

      val joinedSeqExp = seqExpDF.join(seqExpGenomicFileDF, Seq("sequencing_experiment_id"))
        .drop("sequencing_experiment_id")
        .groupBy("file_id").agg(collect_list("sequencing_experiment") as "sequencing_experiment")

      val fileWithSeqExp = df.join(joinedSeqExp, Seq("file_id"), "left")
      val sequencingExperimentFallback = array(
        struct(
          lit(null).cast("string") as "sequencing_experiment_id",
          lit(null).cast("string") as "experiment_date",
          col("experiment_strategy"),
          lit(null).cast("string") as "center",
          lit(null).cast("string") as "library_name",
          lit(null).cast("string") as "library_prep",
          lit(null).cast("string") as "library_selection",
          lit(null).cast("string") as "library_strand",
          lit(null).cast("boolean") as "is_paired_end",
          lit(null).cast("string") as "platform",
          lit(null).cast("string") as "instrument_model",
          lit(null).cast("long") as "max_insert_size",
          lit(null).cast("double") as "mean_insert_size",
          lit(null).cast("double") as "mean_depth",
          lit(null).cast("long") as "total_reads",
          lit(null).cast("double") as "mean_read_length",
          lit(null).cast("string") as "external_id",
          lit(null).cast("string") as "sequencing_center_id"

      ))
      fileWithSeqExp
        .withColumn("sequencing_experiment_fallback", when(col("experiment_strategy").isNotNull, sequencingExperimentFallback).otherwise(null))
        .withColumn("sequencing_experiment", coalesce(col("sequencing_experiment"), col("sequencing_experiment_fallback")))
        .drop("sequencing_experiment_fallback", "experiment_strategy")

    }
    def addBiospecimenFiles(filesDf: DataFrame): DataFrame = {
      val filesWithFacetIds = reformatFileFacetIds(filesDf)

      val fileColumns = filesWithFacetIds.columns.collect { case c if c != "specimen_fhir_ids" => col(c) }
      val reformatFile = filesWithFacetIds
        .withColumn("biospecimen_fhir_id", explode(col("specimen_fhir_ids")))
        .drop("document_reference_fhir_id")
        .withColumn("file", struct(fileColumns: _*))
        .select("biospecimen_fhir_id", "file")
        .groupBy("biospecimen_fhir_id")
        .agg(collect_list(col("file")) as "files")

      df
        .join(reformatFile, df("fhir_id") === reformatFile("biospecimen_fhir_id"), "left_outer")
        .withColumn("files", coalesce(col("files"), array()))
        .withColumn("nb_files", coalesce(size(col("files")), lit(0)))
        .drop("biospecimen_fhir_id")
    }

    def addBiospecimenParticipant(participantsDf: DataFrame): DataFrame = {
      val reformatParticipant: DataFrame = participantsDf
        .withColumn("participant", struct(participantsDf.columns.map(col): _*))
        .withColumn("participant_fhir_id", col("fhir_id"))
        .select("participant_fhir_id", "participant")

      df.join(reformatParticipant, "participant_fhir_id")
    }

    def addFamily(familyDf: DataFrame, familyRelationshipDf: DataFrame): DataFrame = {

      val participantIdMapping = df.select(col("fhir_id") as "participant_fhir_id", col("participant_id"))

      val reformatFamilyRelationship = familyRelationshipDf
        .join(participantIdMapping, col("participant1_fhir_id") === col("participant_fhir_id"))
        .withColumnRenamed("participant_id", "participant1_id")
        .select("participant1_fhir_id", "participant1_id", "participant2_fhir_id", "participant1_to_participant_2_relationship")

      val reformatFamily = familyDf
        .select(col("family_members_id"), col("fhir_id") as "family_fhir_id", col("family_id"))

      val joinFamily = reformatFamily.join(reformatFamilyRelationship, array_contains(col("family_members_id"), col("participant1_fhir_id")), "left_outer")

      val windowSpec = Window.partitionBy("family_id")

      val family = joinFamily.groupBy("participant2_fhir_id", "family_id").agg(
        collect_list(
          struct(
            col("participant1_fhir_id") as "related_participant_fhir_id",
            col("participant1_id") as "related_participant_id",
            col("participant1_to_participant_2_relationship") as "relation"
          )
        ) as "relations",
        first("family_members_id") as "family_members_id",
        first("family_fhir_id") as "family_fhir_id"
      )
        .withColumn("all_relations", collect_set(col("relations.relation")).over(windowSpec))
        .withColumn("has_father", exists(col("all_relations"), relation => array_contains(relation, "father")))
        .withColumn("has_mother", exists(col("all_relations"), relation => array_contains(relation, "mother")))
        .withColumn("has_both_parent", exists(col("all_relations"), relation => array_contains(relation, "father") && array_contains(relation, "mother")))
        .withColumn("nb_members", size(col("family_members_id")))
        .withColumn("family_type",
          when(col("has_both_parent") && col("nb_members") >= 3,
            when(col("nb_members") === 3, "trio").otherwise("trio+")
          ).when((col("has_father") || col("has_mother")) && col("nb_members") >= 2,
            when(col("nb_members") === 2, "duo").otherwise("duo+")
          ).when(col("nb_members") === 0, "proband-only")
            .otherwise("other")
        )
        .withColumn("family", struct(
          filter(col("relations"), c => c("relation") === "father")(0)("related_participant_id") as "father_id",
          filter(col("relations"), c => c("relation") === "mother")(0)("related_participant_id") as "mother_id",
          col("relations") as "family_relations",
          col("family_fhir_id") as "fhir_id",
          col("family_id") as "family_id"
        ))
        .select("family_type", "family","participant2_fhir_id")

      df.join(family, col("fhir_id") === col("participant2_fhir_id"), "left_outer")
        .drop("participant2_fhir_id")
        .withColumn("family_type", coalesce(col("family_type"), lit("proband-only")))
        .join(reformatFamily, array_contains(col("family_members_id"), col("fhir_id")), "left_outer")
        .drop("family_fhir_id", "family_members_id")
        .withColumnRenamed("family_id", "families_id")

    }
  }
}
