package bio.ferlab.fhir.etl.common

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, col, collect_list, filter, map, size,struct, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {


  val hpoPhenotype: UserDefinedFunction =
    udf((code: String, observed: String, source_text: String) => observed.toLowerCase.trim match {
      case "positive" => (s"$source_text ($code)", null, source_text, null, true)
      case _ => (null, s"$source_text ($code)", null, source_text, false)
    })

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

      df.join(reformatBiospecimen, col("fhir_id") === col("participant_fhir_id"))
    }

    def addDiagnosysPhenotypes(conditionDf: DataFrame): DataFrame = {
      val phenotypes = conditionDf
        .select("*").where("""condition_profile == "phenotype"""")
        //filter out phenopype with empty code
        .filter(size(col("condition_coding")) > 0)
        .withColumn("phenotype_code_text", hpoPhenotype(col("condition_coding")(0)("_2"), col("observed"), col("source_text")))
        .withColumn("hpo_phenotype_observed", col("phenotype_code_text")("_1"))
        .withColumn("hpo_phenotype_not_observed", col("phenotype_code_text")("_2"))
        .withColumn("hpo_phenotype_observed_text", col("phenotype_code_text")("_3"))
        .withColumn("hpo_phenotype_not_observed_text", col("phenotype_code_text")("_4"))
        .withColumn("observed_bool", col("phenotype_code_text")("_5"))
        .groupBy("participant_fhir_id")
        .agg(
          collect_list(struct(
            col("fhir_id"),
            col("hpo_phenotype_observed"),
            col("hpo_phenotype_not_observed"),
            col("hpo_phenotype_observed_text"),
            col("hpo_phenotype_not_observed_text"),
            col("observed_bool") as "observed"
          )) as "phenotype"
        )

      val diagnosis: DataFrame = conditionDf
        .select("*").where("""condition_profile == "disease"""")
      phenotypes.show(false)
//      phenotypes.printSchema()
      diagnosis.show(false)


      df
    }
  }


}
