package bio.ferlab.fhir.etl.common

import bio.ferlab.fhir.etl.common.Utils.{hpoPhenotype, observableTitleStandard}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object OntologyUtils {
  val SCHEMA_OBSERVABLE_TERM = "array<struct<name:string,parents:array<string>,is_tagged:boolean,is_leaf:boolean,age_at_event_days:int>>"

  val transformAncestors: UserDefinedFunction =
    udf((arr: Seq[(String, String, Seq[String])], age_at_event: Int) => arr.map(a => (s"${a._2} (${a._1})", a._3, false, false, age_at_event)))

  val transformTaggedTerm: UserDefinedFunction =
    udf((id: String, name: String, parents: Seq[String], is_leaf: Boolean, age_at_event: Int) =>  (s"${name} (${id})", parents, true, is_leaf, age_at_event))

  val groupObservableTermsByAgeAtEvent: UserDefinedFunction =
    udf((arr: Seq[Seq[(String, Seq[String], Boolean, Boolean, Int)]]) => arr)

  def addDiseases(df: DataFrame): DataFrame= {
    val conditionDfColumns = df.columns
    df
      //filter out disease with empty code
      .filter(size(col("condition_coding")) > 0)
      .withColumn("exploded_condition_coding", explode(col("condition_coding")))
      //Assumption -> age_at_event is in days from birth
      .withColumn("age_at_event", col("age_at_event.value"))
      .withColumn("icd_id_diagnosis",
        when(col("exploded_condition_coding")("category") === "ICD",
          col("exploded_condition_coding")("code"))
      )
      .withColumn("mondo_id_diagnosis",
        when(col("exploded_condition_coding")("category") === "MONDO",
          observableTitleStandard(col("exploded_condition_coding")("code")))
      )
      .withColumn("ncit_id_diagnosis",
        when(col("exploded_condition_coding")("category") === "NCIT",
          col("exploded_condition_coding")("code"))
      )
      .groupBy(conditionDfColumns.head, conditionDfColumns.tail: _*)
      .agg(
        first("icd_id_diagnosis", ignoreNulls = true) as "icd_id_diagnosis",
        first("mondo_id_diagnosis", ignoreNulls = true) as "mondo_id_diagnosis",
        first("ncit_id_diagnosis", ignoreNulls = true) as "ncit_id_diagnosis"
      )
      .withColumnRenamed("age_at_event", "age_at_event_days")
      .drop("condition_coding", "release_id")
  }

  def addPhenotypes(df: DataFrame): DataFrame= {
    df
      //filter out phenopype with empty code
      .filter(size(col("condition_coding")) > 0)
      .withColumn("phenotype_code_text",
        hpoPhenotype(
          col("condition_coding")(0)("code"),
          when(col("observed").isNull, "negative")
            .otherwise(col("observed")),
          col("source_text"),
          //Assumption -> units is always days and its observed from birth.
          col("age_at_event.value") as "age_at_event_days",
        )
      )
      .withColumn("hpo_phenotype_observed", col("phenotype_code_text")("_1"))
      .withColumn("hpo_phenotype_not_observed", col("phenotype_code_text")("_2"))
      .withColumn("hpo_phenotype_observed_text", col("phenotype_code_text")("_3"))
      .withColumn("hpo_phenotype_not_observed_text", col("phenotype_code_text")("_4"))
      .withColumn("observed_bool", col("phenotype_code_text")("_5"))
      .withColumn("observable_term", observableTitleStandard(col("condition_coding")(0)("code")))
      .withColumn("age_at_event_days", col("phenotype_code_text")("_6"))
  }

  def mapObservableTerms(df: DataFrame, pivotColumn: String)(observableTerms: DataFrame): DataFrame = {
    df
      .join(observableTerms, col(pivotColumn) === col("id"), "left_outer")
      .withColumn("transform_ancestors", when(col("ancestors").isNotNull,  transformAncestors(col("ancestors"), col("age_at_event_days"))))
      .withColumn("transform_tagged_observable", transformTaggedTerm(col("id"), col("name"),col("parents"), col("is_leaf"),  col("age_at_event_days")))
      .withColumn("observable_with_ancestors", array_union(col("transform_ancestors"), array(col("transform_tagged_observable"))))
      .withColumn("observable_with_ancestors",
        when(col("observable_with_ancestors").isNull, array().cast(SCHEMA_OBSERVABLE_TERM))
          .otherwise(col("observable_with_ancestors").cast(SCHEMA_OBSERVABLE_TERM))
      )
      .drop("transform_ancestors", "transform_tagged_observable", "ancestors", "id", "is_leaf", "name", "parents")
  }

  def groupObservableTermsByAge(df: DataFrame, observableTermColName: String): DataFrame= {
    df
      .select(
        "participant_fhir_id",
        s"${observableTermColName}.name",
        s"${observableTermColName}.parents",
        s"${observableTermColName}.is_tagged",
        s"${observableTermColName}.is_leaf",
        s"${observableTermColName}.age_at_event_days",
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
