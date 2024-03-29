package bio.ferlab.etl.normalized.clinical

import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.etl.normalized.clinical.Utils._
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UtilsSpec extends AnyFlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  "retrieveRepository" should "return dcf, gen3 or null" in {
    val df = Seq(
      s"https://$gen3Domain/path",
      s"drs://$dcfDomain/path",
      s"https://$dcfDomain2/path",
      "other",
      null
    ).toDF("repository")

    df.select(retrieveRepository(col("repository"))).as[String].collect() should contain theSameElementsAs Seq("gen3", "dcf", "dcf", null, null)
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

  "extractDocumentReferenceAcl" should "extract acl and modify open access values if exist" in {
    val rawDF = Seq(
      ("f1", Seq("false", "*"), "s"),
      ("f2", Seq(null, "Registered"), "s"),
      ("f3", Seq("true", "phs002330.c1"), "s"),
      ("f3", Seq("true", "s"), "s"),
    ).toDF("file_id", "raw_acl", "study_id")

    val df = rawDF
      .withColumn("acl", extractDocumentReferenceAcl(col("raw_acl"), col("study_id")))

    df
      .select(col("file_id"), col("acl")).as[(String, Seq[String])].collect() should contain theSameElementsAs Seq(
      ("f1", Seq("open_access")),
      ("f2", Seq("open_access")),
      ("f3", Seq("phs002330.c1")),
      ("f3", Seq("s")),
    )
  }

  "retrieveSize" should "transform a string to a long" in {
    // getBytes() is used since "fileSize" is defined as bytes in src/main/resources/schema/kfdrc-documentreference.avsc
    val df = Seq(
      "17290282717".getBytes(),
      "1451170".getBytes(),
      // Testing overflow
      "2337555464123423423445".getBytes(),
      null
    ).toDF("size")

    df.select(retrieveSize(col("size"))).as[Option[Long]].collect() should contain theSameElementsAs Seq(
      Some(17290282717L),
      Some(1451170L),
      None,
      None
    )
  }

  "extractStudyVersion" should "extract the version" in {
    val df = Seq(
      "phs002330.v1.p1",
      null,
      "abcdefgh",
      "stu.dy",
      "this.is.a.very.long.example",
      "this.has,weird.characters"
    ).toDF("version")

    df.select(extractStudyVersion(col("version"))).as[Option[String]].collect() should contain theSameElementsAs Seq(
      Some("v1.p1"),
      None,
      Some(""),
      Some("dy"),
      Some("is.a.very.long.example"),
      Some("has,weird.characters")
    )
  }

  "extractStudyExternalId" should "extract the external id" in {
    val df = Seq(
      "phs002330.v1.p1",
      null,
      "abcdefgh",
      "stu.dy",
    ).toDF("external_id")

    df.select(extractStudyExternalId(col("external_id"))).as[Option[String]].collect() should contain theSameElementsAs Seq(
      Some("phs002330"),
      None,
      Some("abcdefgh"),
      Some("stu"),
    )
  }
}
