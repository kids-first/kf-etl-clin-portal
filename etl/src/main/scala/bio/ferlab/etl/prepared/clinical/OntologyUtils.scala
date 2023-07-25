package bio.ferlab.etl.prepared.clinical

import bio.ferlab.etl.prepared.clinical.Utils.observableTitleStandard
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object OntologyUtils {

  val displayTerm: (Column, Column) => Column = (id, name) => concat(name, lit(" ("), id, lit(")"))
  val transformAncestors: (Column, Column) => Column = (ancestors, age) => transform(ancestors, a =>
    struct(
      displayTerm(a("id"), a("name")) as "name",
      a("parents") as "parents",
      lit(false) as "is_tagged",
      lit(false) as "is_leaf",
      age as "age_at_event_days"
    )
  )

  val transformTaggedTerm: (Column, Column, Column, Column, Column) => Column = (id, name, parents, is_leaf, age) =>
    struct(
      displayTerm(id, name) as "name",
      parents as "parents",
      lit(true) as "is_tagged",
      is_leaf as "is_leaf",
      age as "age_at_event_days"
    )

  val firstCategory: (String, Column) => Column = (category, codes) => filter(codes, code => code("category") === lit(category))(0)("code")

  def addDiseases(diseases: DataFrame, mondoTerms: DataFrame): DataFrame = {
    val mondoTermsIdName = mondoTerms.select(col("id") as "mondo_term_id", col("name") as "mondo_name")
    diseases
      //filter out disease with empty code
      .where(size(col("condition_coding")) > 0)
      .withColumn("icd_id_diagnosis", firstCategory("ICD", col("condition_coding")))
      .withColumn("ncit_id_diagnosis", firstCategory("NCIT", col("condition_coding")))
      //Assumption -> age_at_event is in days from birth
      .withColumn("age_at_event_days", col("age_at_event.value"))
      .drop("condition_coding", "release_id")
      .join(mondoTermsIdName, col("mondo_id") === mondoTermsIdName("mondo_term_id"), "left_outer")
      .withColumn("mondo_id_diagnosis", displayTerm(col("mondo_id"), col("mondo_name")))
      .drop("mondo_term_id","mondo_name")
  }

  def addPhenotypes(df: DataFrame, hpoTerms: DataFrame): DataFrame = {
    val hpoIdName =  hpoTerms.select("id", "name")
      .withColumnRenamed("id", "observable_term")

    df.where(size(col("condition_coding")) > 0)
      .withColumn("is_observed", col("observed").isNotNull && col("observed") === "confirmed")
      .withColumn("observable_term", observableTitleStandard(firstCategory("HPO", col("condition_coding"))))
      .join(hpoIdName, Seq("observable_term"), "left_outer")
      .withColumn("age_at_event_days", col("age_at_event.value"))
      .withColumn("formatted_hpo", concat(col("name"), lit(" ("), col("observable_term"), lit(")")))
      .withColumn("hpo_phenotype_observed", when(col("is_observed"), col("formatted_hpo")).otherwise(null))
      .withColumn("hpo_phenotype_not_observed", when(not(col("is_observed")), col("formatted_hpo")).otherwise(null))
      .drop("name", "formatted_hpo")
  }

  def mapObservableTerms(df: DataFrame, pivotColumn: String)(observableTerms: DataFrame): DataFrame = {
    df
      .join(observableTerms, col(pivotColumn) === col("id"), "left_outer")
      .withColumn("transform_ancestors", when(col("ancestors").isNotNull, transformAncestors(col("ancestors"), col("age_at_event_days"))))
      .withColumn("transform_tagged_observable", transformTaggedTerm(col("id"), col("name"), col("parents"), col("is_leaf"), col("age_at_event_days")))
      .withColumn("observable_with_ancestors", array_union(col("transform_ancestors"), array(col("transform_tagged_observable"))))
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
