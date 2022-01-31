package bio.ferlab.fhir.etl.common

import bio.ferlab.fhir.etl.common.OntologyUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object Utils {

  val hpoPhenotype: UserDefinedFunction =
    udf((code: String, observed: String, source_text: String, age_at_event_days: Int) => observed.toLowerCase.trim match {
      //  hpo_observed/hpo_non_observed/hpo_observed_text/hpo_non_observed_text/observed_bool
      case "positive" => (s"$source_text ($code)", null, source_text, null, true, age_at_event_days)
      case _ => (null, s"$source_text ($code)", null, source_text, false, age_at_event_days)
    })

  val observableTiteStandard: UserDefinedFunction =
    udf((term: String) => term.trim.replace("_", ":"))


  implicit class DataFrameOperations(df: DataFrame) {
    def addStudy(studyDf: DataFrame): DataFrame = {
      val reformatStudy: DataFrame = studyDf
        .withColumn("study", struct(studyDf.columns.filter(!_.equals("release_id")).map(col): _*))
        .select("study_id", "study")

      df.join(reformatStudy, "study_id")
    }

    def addBiospecimen(biospecimensDf: DataFrame): DataFrame = {
      //todo - prior - add sequencing experiment to genomic files
      //todo add genomic files to biospecimen
      val reformatBiospecimen: DataFrame = biospecimensDf
        .withColumn("biospecimen", struct(biospecimensDf.columns.map(col): _*))
        .select("participant_fhir_id", "biospecimen")
        .groupBy("participant_fhir_id")
        .agg(
          collect_list(col("biospecimen")) as "biospecimens")

      val arrBioSchema = reformatBiospecimen.schema(1).dataType

      df
        .join(reformatBiospecimen, col("fhir_id") === col("participant_fhir_id"), "left_outer")
        .withColumn("biospecimens", when(col("biospecimens").isNull, array().cast(arrBioSchema)).otherwise(col("biospecimens")))
        .drop("participant_fhir_id")
    }

    def addOutcomes(vitalStatusDf: DataFrame): DataFrame = {
      val reformatObservation: DataFrame = vitalStatusDf
        .withColumn("outcome", struct(vitalStatusDf.columns.map(col): _*))
        .select("participant_fhir_id", "outcome")
        .groupBy("participant_fhir_id")
        .agg(
          collect_list(col("outcome").dropFields("study_id", "release_id")) as "outcomes")

      df
        .join(reformatObservation, col("fhir_id") === col("participant_fhir_id"), "left_outer")
        .drop( "participant_fhir_id")
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
              col("hpo_phenotype_observed_text"),
              col("hpo_phenotype_not_observed_text"),
              col("observed_bool") as "observed",
              col("age_at_event_days")
            )) as "phenotype",
            collect_list(
              when(lower(col("observed")) === "positive", col("observable_with_ancestors"))
            ) as "observed_phenotype",
            collect_list(
              when(lower(col("observed")) =!= "positive" || col("observed").isNull, col("observable_with_ancestors"))
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

      val arrPhenotypeSchema = phenotypesWithHPOTermsGroupedByEvent.schema("phenotype").dataType

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
        .withColumn("phenotype", when(col("phenotype").isNull, array().cast(arrPhenotypeSchema)).otherwise(col("phenotype")))
        .drop("participant_fhir_id")
        .join(diseasesWithReplacedMondoTerms, col("fhir_id") === col("participant_fhir_id"), "left_outer")
        .drop("participant_fhir_id")

    }

    def addFiles(filesDf: DataFrame): DataFrame = {
      val columnsGroup = Seq("study_id", "participant_fhir_id")
      val columnsFile = filesDf.columns.filter(col => !columnsGroup.contains(col) )
      val filesPerParticipant =
        filesDf
          .groupBy(columnsGroup.head, columnsGroup.tail: _*)
          .agg(
            collect_list(
              struct(
                columnsFile.head,
                columnsFile.tail: _*
              )
            ) as "files",
          )
          .drop("study_id")

      df
        .join(filesPerParticipant, col("fhir_id") === col("participant_fhir_id"), "left_outer")
        .drop("participant_fhir_id")
    }

    def addFamily(familyDf: DataFrame): DataFrame = {
      val reformatFamily: DataFrame = familyDf
        .withColumn("family", struct(familyDf.columns.map(col): _*))
        .select("family_members_id", "family_id", "family")

      val groupCols = df.columns.diff(Seq("family_id", "family"))

      df.join(reformatFamily, expr("array_contains(family_members_id,fhir_id)"), "left_outer")
        .groupBy(groupCols.map(col): _*)
        .agg(collect_list("family_id") as "families_id", collect_list(col("family").dropFields("family_members_id", "release_id")) as "families")
    }

    def addParticipant(participantsDf: DataFrame): DataFrame = {
      val reformatParticipant: DataFrame = participantsDf
        .withColumn("participant", struct(participantsDf.columns.map(col): _*))
        .withColumnRenamed("fhir_id", "participant_fhir_id")
        .select("participant_fhir_id", "participant")

      df.join(reformatParticipant, "participant_fhir_id")
    }

    def addParticipants(participantsDf: DataFrame): DataFrame = {
      val reformatParticipant: DataFrame = participantsDf
        .withColumn("participants", array(struct(participantsDf.columns.map(col): _*)))
        .withColumnRenamed("fhir_id", "participant_fhir_id")
        .select("participant_fhir_id", "participants")

      df.join(reformatParticipant, "participant_fhir_id")
    }
  }
}
