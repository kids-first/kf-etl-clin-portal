package bio.ferlab.fhir.etl.common

import bio.ferlab.fhir.etl.common.Utils.observableTiteStandard
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object OntologyUtils {
  //TODO add age_at_event_days field
  val SCHEMA_OBSERVABLE_TERM = "array<struct<name:string,parents:array<string>,is_tagged:boolean,is_leaf:boolean>>"

  //TODO add age_at_event_days field
  val transformAncestors: UserDefinedFunction =
    udf((arr: Seq[(String, String, Seq[String])]) => arr.map(a => (s"${a._2} (${a._1})", a._3, false, false)))

  //TODO add age_at_event_days field
  val transformTaggedTerm: UserDefinedFunction =
    udf((id: String, name: String, parents: Seq[String], is_leaf: Boolean) =>  (s"${name} (${id})", parents, true, is_leaf))

  def addDiseases(df: DataFrame): DataFrame= {
    val conditionDfColumns = df.columns
    df
      .select("*").where("""condition_profile == "disease"""")
      //filter out disease with empty code
      .filter(size(col("condition_coding")) > 0)
      .withColumn("exploded_condition_coding", explode(col("condition_coding")))
      .withColumn("icd_id_diagnosis",
        when(col("exploded_condition_coding")("category") === "ICD",
          col("exploded_condition_coding")("code"))
      )
      .withColumn("mondo_id_diagnosis",
        when(col("exploded_condition_coding")("category") === "MONDO",
          observableTiteStandard(col("exploded_condition_coding")("code")))
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
      .drop("condition_coding")
  }

  def mapObservableTerms(df: DataFrame, pivotColumn: String)(observableTerms: DataFrame): DataFrame= {
    df
//      .filter("mondo_id_diagnosis is not null") //FIXME REMOVE
      .join(observableTerms, col(pivotColumn) === col("id"), "left_outer")
      .withColumn("transform_ancestors", when(col("ancestors").isNotNull,  transformAncestors(col("ancestors"))))
      .withColumn("transform_tagged_observable", transformTaggedTerm(col("id"), col("name"),col("parents"), col("is_leaf") ))
      .withColumn("observable_with_ancestors", array_union(col("transform_ancestors"), array(col("transform_tagged_observable"))))
      .withColumn("observable_with_ancestors", col("observable_with_ancestors").cast(SCHEMA_OBSERVABLE_TERM))
      .drop("transform_ancestors", "transform_tagged_observable", "ancestors", "id", "is_leaf", "name", "parents")
  }
}
