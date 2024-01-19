package bio.ferlab.etl

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{filter, lit, regexp_replace, trim}

import java.time.LocalDateTime

object Utils {
  val firstCategory: (String, Column) => Column = (category, codes) => filter(codes, code => code("category") === lit(category))(0)("code")
  val observableTitleStandard: Column => Column = term => trim(regexp_replace(term, "_", ":"))
  val minDateTime: LocalDateTime = LocalDateTime.of(1900, 1, 1, 0, 0, 0)
}
