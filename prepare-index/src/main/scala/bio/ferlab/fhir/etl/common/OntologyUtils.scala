package bio.ferlab.fhir.etl.common

import bio.ferlab.fhir.etl.common.Utils.observableTitleStandard
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object OntologyUtils {
  val SCHEMA_OBSERVABLE_TERM = "array<struct<name:string,parents:array<string>,is_tagged:boolean,is_leaf:boolean,age_at_event_days:int>>"

  val transformAncestors: UserDefinedFunction =
    udf((arr: Seq[(String, String, Seq[String])], age_at_event: Int) => arr.map(a => (s"${a._2} (${a._1})", a._3, false, false, age_at_event)))

  val transformTaggedTerm: UserDefinedFunction =
    udf((id: String, name: String, parents: Seq[String], is_leaf: Boolean, age_at_event: Int) => (s"${name} (${id})", parents, true, is_leaf, age_at_event))

  val firstCategory: (String, Column) => Column = (category, codes) => filter(codes, code => code("category") === lit(category))(0)("code")


  def addDiseases(df: DataFrame): DataFrame = {
    df
      //filter out disease with empty code
      .where(size(col("condition_coding")) > 0)
      .withColumn("icd_id_diagnosis", firstCategory("ICD", col("condition_coding")))
      .withColumn("ncit_id_diagnosis", firstCategory("NCIT", col("condition_coding")))
      .withColumn("mondo_id_diagnosis", observableTitleStandard(firstCategory("MONDO", col("condition_coding"))))
      //Assumption -> age_at_event is in days from birth
      .withColumn("age_at_event_days", col("age_at_event.value"))
      .drop("condition_coding", "release_id")
  }

  def addPhenotypes(df: DataFrame): DataFrame = {
    df.where(size(col("condition_coding")) > 0)
      .withColumn("is_observed", col("observed").isNotNull && col("observed") === "confirmed")
      .withColumn("observable_term", observableTitleStandard(firstCategory("HPO", col("condition_coding"))))
      .withColumn("age_at_event_days", col("age_at_event.value"))
      .withColumn("formatted_hpo", concat(col("source_text"), lit(" ("), col("observable_term"), lit(")")))
      .withColumn("hpo_phenotype_observed", when(col("is_observed"), col("formatted_hpo")).otherwise(null))
      .withColumn("hpo_phenotype_not_observed", when(not(col("is_observed")), col("formatted_hpo")).otherwise(null))
      .drop("formatted_hpo")
  }

  def mapObservableTerms(df: DataFrame, pivotColumn: String)(observableTerms: DataFrame): DataFrame = {
    df
      .join(observableTerms, col(pivotColumn) === col("id"), "left_outer")
      .withColumn("transform_ancestors", when(col("ancestors").isNotNull, transformAncestors(col("ancestors"), col("age_at_event_days"))))
      .withColumn("transform_tagged_observable", transformTaggedTerm(col("id"), col("name"), col("parents"), col("is_leaf"), col("age_at_event_days")))
      .withColumn("observable_with_ancestors", array_union(col("transform_ancestors"), array(col("transform_tagged_observable"))))
      .withColumn("observable_with_ancestors",
        when(col("observable_with_ancestors").isNull, array().cast(SCHEMA_OBSERVABLE_TERM))
          .otherwise(col("observable_with_ancestors").cast(SCHEMA_OBSERVABLE_TERM))
      )
      .drop("transform_ancestors", "transform_tagged_observable", "ancestors", "id", "is_leaf", "name", "parents")
  }

  def groupObservableTermsByAge(df: DataFrame, observableTermColName: String): DataFrame = {
    df
      .select(
        "participant_fhir_id",
        s"$observableTermColName.name",
        s"$observableTermColName.parents",
        s"$observableTermColName.is_tagged",
        s"$observableTermColName.is_leaf",
        s"$observableTermColName.age_at_event_days",
      )
      .groupBy("participant_fhir_id", "name", "parents", "is_tagged", "is_leaf")
      .agg(
        col("participant_fhir_id"),
        struct(
          col("name"),
          col("parents"),
          col("is_tagged"),
          col("is_leaf"),
          collect_set(col("age_at_event_days")) as "age_at_event_days"
        ) as observableTermColName,
      )
      .groupBy("participant_fhir_id")
      .agg(
        collect_list(observableTermColName) as observableTermColName
      )
  }
}
