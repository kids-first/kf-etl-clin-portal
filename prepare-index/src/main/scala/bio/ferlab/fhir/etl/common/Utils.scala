package bio.ferlab.fhir.etl.common

import bio.ferlab.fhir.etl.common.OntologyUtils._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, array_union, col, collect_list, explode, expr, filter, first, lit, map, size, struct, transform, udf, when}
import org.apache.spark.sql.DataFrame

object Utils {

  val hpoPhenotype: UserDefinedFunction =
    udf((code: String, observed: String, source_text: String) => observed.toLowerCase.trim match {
      //  hpo_observed/hpo_non_observed/hpo_observed_text/hpo_non_observed_text/observed_bool
      case "positive" => (s"$source_text ($code)", null, source_text, null, true)
      case _ => (null, s"$source_text ($code)", null, source_text, false)
    })

  val observableTiteStandard: UserDefinedFunction =
    udf((term: String) => term.trim.replace("_", ":"))


  implicit class DataFrameOperations(df: DataFrame) {
    def addStudy(studyDf: DataFrame): DataFrame = {
      val reformatStudy: DataFrame = studyDf
        .withColumn("study", struct(studyDf.columns.map(col): _*))
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


    def addOutcomes(observationsDf: DataFrame): DataFrame = {
      val reformatObservation: DataFrame = observationsDf
        .withColumn("outcome", struct(observationsDf.columns.map(col): _*))
        .select("participant_fhir_id", "outcome")
        .groupBy("participant_fhir_id")
        .agg(
          collect_list(col("outcome")) as "outcomes")

      df
        .join(reformatObservation, col("fhir_id") === col("participant_fhir_id"))
        .drop( "participant_fhir_id")
    }

    def addDiagnosisPhenotypes(conditionDf: DataFrame)(hpoTerms: DataFrame, mondoTerms: DataFrame): DataFrame = {

      val phenotypes = conditionDf
        .select("*").where("""condition_profile == "phenotype"""")
        //filter out phenopype with empty code
        .filter(size(col("condition_coding")) > 0)
        .withColumn("phenotype_code_text",
          hpoPhenotype(
            col("condition_coding")(0)("code"),
            when(col("observed").isNull, "negative")
              .otherwise(col("observed")),
            col("source_text")
          )
        )
        .withColumn("hpo_phenotype_observed", col("phenotype_code_text")("_1"))
        .withColumn("hpo_phenotype_not_observed", col("phenotype_code_text")("_2"))
        .withColumn("hpo_phenotype_observed_text", col("phenotype_code_text")("_3"))
        .withColumn("hpo_phenotype_not_observed_text", col("phenotype_code_text")("_4"))
        .withColumn("observed_bool", col("phenotype_code_text")("_5"))
        .withColumn("observable_term", observableTiteStandard(col("condition_coding")(0)("code")))

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
              col("observed_bool") as "observed"
            )) as "phenotype",
            collect_list(
              when(col("observed") === "Positive", col("observable_with_ancestors"))
            ) as "observed_phenotype",
            collect_list(
              when(col("observed") === "Negative", col("observable_with_ancestors"))
            ) as "non_observed_phenotype"
          )

      val arrPhenotypeSchema = phenotypesWithHPOTerms.schema("phenotype").dataType

      val diseases = addDiseases(conditionDf)
      val commonColumns = Seq("fhir_id", "study_id")
      val diseaseColumns = diseases.columns.filter(col => !commonColumns.contains(col))

      val diseasesWithMondoTerms =
        mapObservableTerms(diseases, "mondo_id_diagnosis")(mondoTerms)
          .withColumn("mondo", explode(col("observable_with_ancestors")))
          .drop(commonColumns :+ "observable_with_ancestors": _*)
          .groupBy("participant_fhir_id")
          .agg(
            collect_list(
              struct(
                diseaseColumns.head,
                diseaseColumns.tail :+ "mondo": _*
              )
            ) as "diagnoses",
          )

      df
        .join(phenotypesWithHPOTerms, col("fhir_id") === col("participant_fhir_id"), "left_outer")
        .withColumn("phenotype", when(col("phenotype").isNull, array().cast(arrPhenotypeSchema)).otherwise(col("phenotype")))
        .drop("participant_fhir_id")
        .join(diseasesWithMondoTerms, col("fhir_id") === col("participant_fhir_id"), "left_outer")
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
        .agg(collect_list("family_id") as "families_id", collect_list("family") as "families")
        .drop("family_members_id")
    }

    def addParticipant(participantsDf: DataFrame): DataFrame = {
      val reformatParticipant: DataFrame = participantsDf
        .withColumn("participant", struct(participantsDf.columns.map(col): _*))
        .withColumnRenamed("fhir_id", "participant_fhir_id")
        .select("participant_fhir_id", "participant")

      df.join(reformatParticipant, "participant_fhir_id")
    }
  }
}
