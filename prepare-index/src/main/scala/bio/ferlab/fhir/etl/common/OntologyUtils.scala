package bio.ferlab.fhir.etl.common

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object OntologyUtils {

  //TODO add age_at_event_days field
  val SCHEMA_PHENOTYPE = "array<struct<name:string,parents:array<string>,is_tagged:boolean,is_leaf:boolean>>"

  //TODO add age_at_event_days field
  val transformAncestors: UserDefinedFunction =
    udf((arr: Seq[(String, String, Seq[String])]) => arr.map(a => (s"${a._2} (${a._1})", a._3, false, false)))

  //TODO add age_at_event_days field
  val transformTaggedPhenotype: UserDefinedFunction =
    udf((id: String, name: String, parents: Seq[String], is_leaf: Boolean) =>  (s"${name} (${id})", parents, true, is_leaf))

}
