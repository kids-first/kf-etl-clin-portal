package bio.ferlab.fhir.etl.common

import bio.ferlab.fhir.etl.common.OntologyUtils.{transformAncestors, transformTaggedPhenotype}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, array_union, col, collect_list, filter, lit, map, size, struct, transform, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {


  val hpoPhenotype: UserDefinedFunction =
    udf((code: String, observed: String, source_text: String) => observed.toLowerCase.trim match {
      case "positive" => (s"$source_text ($code)", null, source_text, null, true)
      case _ => (null, s"$source_text ($code)", null, source_text, false)
    })

  val observableTileStandard: UserDefinedFunction =
    udf((term: String) => term.replace("_", ":"))

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
        .drop("fhir_id")
    }

    def addDiagnosysPhenotypes(conditionDf: DataFrame)(hpoTerms: DataFrame): DataFrame = {
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
        .withColumn("observable_term", observableTileStandard(col("condition_coding")(0)("_2")))
        .join(hpoTerms, col("observable_term") === col("id"))
        .withColumn("transform_ancestors", transformAncestors(col("ancestors")))
        .withColumn("transform_tagged_phenotype", transformTaggedPhenotype(col("id"), col("name"),col("parents"), col("is_leaf") ))
        .withColumn("phenotype_with_ancestors", array_union(col("transform_ancestors"), array(col("transform_tagged_phenotype"))))
        .drop("transform_ancestors", "transform_tagged_phenotype", "ancestors", "id", "is_leaf", "name", "parents")
      //        .groupBy("participant_fhir_id")
//        .agg(
//          collect_list(struct(
//            col("fhir_id"),
//            col("hpo_phenotype_observed"),
//            col("hpo_phenotype_not_observed"),
//            col("hpo_phenotype_observed_text"),
//            col("hpo_phenotype_not_observed_text"),
//            col("observed_bool") as "observed"
//          )) as "phenotype"
//        )

//      val toto = df.join(phenotypes, "participant_fhir_id")

      phenotypes.show(false)

      phenotypes

//      val diagnosis: DataFrame = conditionDf
//        .select("*").where("""condition_profile == "disease"""")
//      phenotypes.show(false)
//      df.show(false)
//      phenotypes.printSchema()
//      diagnosis.show(false)


    }

  }


}
