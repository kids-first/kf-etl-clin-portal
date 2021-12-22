package bio.ferlab.fhir.etl.common

import bio.ferlab.fhir.etl.common.OntologyUtils._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, array_union, col, collect_list, explode, filter, first, lit, map, size, struct, transform, udf, when}
import org.apache.spark.sql.DataFrame

object Utils {

  val hpoPhenotype: UserDefinedFunction =
    udf((code: String, observed: String, source_text: String) => observed.toLowerCase.trim match {
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
      val reformatBiospecimen: DataFrame = biospecimensDf
        .withColumn("biospecimen", struct(biospecimensDf.columns.map(col): _*))
        .select("participant_fhir_id", "biospecimen")
        .groupBy("participant_fhir_id")
        .agg(
          collect_list(col("biospecimen")) as "biospecimens")

      df
        .join(reformatBiospecimen, col("fhir_id") === col("participant_fhir_id"))
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
        .drop("fhir_id", "participant_fhir_id")
    }

    def addDiagnosisPhenotypes(conditionDf: DataFrame)(hpoTerms: DataFrame, mondoTerms: DataFrame): DataFrame = {

      val phenotypes = conditionDf
        .select("*").where("""condition_profile == "phenotype"""")
        //filter out phenopype with empty code
        .filter(size(col("condition_coding")) > 0)
        .withColumn("phenotype_code_text", hpoPhenotype(col("condition_coding")(0)("code"), col("observed"), col("source_text")))
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
              when(col("observed") === "Positive", col("phenotype_with_ancestors"))
            ) as "observed_phenotype",
            collect_list(
              when(col("observed") === "Negative", col("phenotype_with_ancestors"))
            ) as "non_observed_phenotype"
          )

      val diseases = addDiseases(conditionDf)
      val diseasesWithMondoTerms = mapObservableTerms(diseases, "mondo_id_diagnosis")(mondoTerms)

      phenotypesWithHPOTerms.show(false)
      diseasesWithMondoTerms.show(false)

      conditionDf
    }
  }
}
