package bio.ferlab.etl.normalized.clinical

import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.etl.normalized.clinical.Utils.{age_on_set, retrieveRepository, sanitizeFilename}
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UtilsSpec extends AnyFlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  "retrieveRepository" should "return dcf, gen3 or null" in {
    val df = Seq("https://data.kidsfirstdrc.org/path", "https://api.gdc.cancer.gov/path", "other", null).toDF("repository")

    df.select(retrieveRepository(col("repository"))).as[String].collect() should contain theSameElementsAs Seq("gen3", "dcf", null, null)
  }

  "sanitizeFilename" should "remove special characters" in {
    val df = Seq("s3://kf-study-us-east-1-prd-sd-z6mwd3h0/source/lupo_all/GMKF_Lupo_CongenitalHeartDefects_WGS_Normals/RP-1922/WGS/P7339_N/v1/P7339_N.cram",
      "156a028f-106b-4341-974b-bbe6a92a1b0f.vardict_somatic.PASS.vep.vcf.gz").toDF("filename")
    df.select(sanitizeFilename($"filename")).as[String].collect() should contain theSameElementsAs Seq("P7339_N.cram", "156a028f-106b-4341-974b-bbe6a92a1b0f.vardict_somatic.PASS.vep.vcf.gz")
  }

  "age_on_set" should "return on set interval adequately" in {
    val df = Seq(0, 12, 38, 50).toDF("age")
    val intervals = Seq((0, 20), (20, 30), (30, 40))
    df.select(age_on_set(col("age"), intervals)).as[String].collect() should contain theSameElementsAs Seq(
      "0 - 20",
      "0 - 20",
      "30 - 40",
      "40+",
    )

  }

}
