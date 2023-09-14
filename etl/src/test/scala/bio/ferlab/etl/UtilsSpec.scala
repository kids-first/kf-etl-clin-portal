package bio.ferlab.etl

import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.etl.Utils.firstCategory
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UtilsSpec extends AnyFlatSpec with Matchers with WithSparkSession {

  case class ConditionCoding(code: String, category: String)
  import spark.implicits._

  val SCHEMA_CONDITION_CODING = "array<struct<category:string,code:string>>"

  "firstCategory" should "return the first found category" in {
    val df = Seq(Seq(("ICD", "icd"))).toDF("condition_coding").withColumn("condition_coding", col("condition_coding").cast(SCHEMA_CONDITION_CODING))
    df.withColumn("cat_icd", firstCategory("ICD", col("condition_coding"))).select("cat_icd").collect().head shouldBe Row("icd")
    df.withColumn("cat_icd", firstCategory("NotFound", col("condition_coding"))).select("cat_icd").collect().head shouldBe Row(null)
  }
}
