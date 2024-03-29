package bio.ferlab.fhir.etl.config

import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.fhir.etl.config.StudyConfiguration.{defaultStudyConfiguration, kfStudiesConfiguration}
import org.apache.spark.sql.functions.{col, explode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class Datum(studyId: String, pattern: String, paths: Seq[String])

class StudyConfigurationSpec extends AnyFlatSpec with Matchers with WithSparkSession {

  import spark.implicits._

  // "data" represents pertinent information found in given excel where patterns are given.
  val data: Seq[Datum] = Seq(
    Datum(
      studyId = "SD_NO_EXISTS_IN_CONF",
      pattern = defaultStudyConfiguration.snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-no-exists-in-conf/harmonized-data/family-variants/e791460c-92df-46d9-83b5-4f49e4223238.postCGP.filtered.deNovo.vep.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_RM8AFW0R",
      pattern = kfStudiesConfiguration("SD_RM8AFW0R").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-rm8afw0r/harmonized-data/family-variants/e791460c-92df-46d9-83b5-4f49e4223238.CGP.filtered.deNovo.vep.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_Z0D9N23X",
      pattern = kfStudiesConfiguration("SD_Z0D9N23X").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-z0d9n23x/harmonized-data/family-variants/e791460c-92df-46d9-83b5-4f49e4223238.CGP.filtered.deNovo.vep.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_46SK55A3",
      pattern = kfStudiesConfiguration("SD_46SK55A3").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-46sk55a3/harmonized/family-variants/e791460c-92df-46d9-83b5-4f49e4223238.CGP.filtered.deNovo.vep.vcf.gz",
        "s3://kf-study-us-east-1-prd-sd-46sk55a3/harmonized/family-variants/e791460c-92df-46d9-83b5-4f49e4223238.postCGP.filtered.deNovo.vep.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_2CEKQ05V",
      pattern = kfStudiesConfiguration("SD_2CEKQ05V").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-2cekq05v/harmonized-data/family-variants/e791460c-92df-46d9-83b5-4f49e4223238.postCGP.filtered.deNovo.vep.vcf.gz",
      )
    ),
    Datum(
      studyId = "SD_54G4WG4R",
      pattern = kfStudiesConfiguration("SD_54G4WG4R").snvVCFPattern,
      paths = Seq(
        "s3://kf-strides-study-us-east-1-prd-sd-54g4wg4r/harmonized-data/family-variants/347dea75-c8a1-47da-87de-16b57c5aa3cd.CGP.filtered.deNovo.vep.vcf.gz",
        "s3://kf-strides-study-us-east-1-prd-sd-54g4wg4r/harmonized-data/family-variants/37fff1d3-9389-4a35-87a0-e554e2a63e1d.multi.vqsr.filtered.denovo.vep_105.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_PREASA7S",
      pattern = kfStudiesConfiguration("SD_PREASA7S").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-preasa7s/harmonized/family-variants/347dea75-c8a1-47da-87de-16b57c5aa3cd.CGP.filtered.deNovo.vep.vcf.gz",
        "s3://kf-study-us-east-1-prd-sd-preasa7s/harmonized/family-variants/347dea75-c8a1-47da-87de-16b57c5aa3cd.postCGP.filtered.deNovo.vep.vcf.gz",
        "s3://kf-study-us-east-1-prd-sd-preasa7s/harmonized-data/family-variants/37fff1d3-9389-4a35-87a0-e554e2a63e1d.CGP.filtered.deNovo.vep.vcf.gz",
        "s3://kf-study-us-east-1-prd-sd-preasa7s/harmonized-data/family-variants/37fff1d3-9389-4a35-87a0-e554e2a63e1d.postCGP.filtered.deNovo.vep.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_Z6MWD3H0",
      pattern = kfStudiesConfiguration("SD_Z6MWD3H0").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-z6mwd3h0/harmonized-data/family-variants/a9dab2bd-3753-420b-b7b8-a845ac06bfe5.CGP.filtered.deNovo.vep.vcf.gz",
        "s3://kf-study-us-east-1-prd-sd-z6mwd3h0/harmonized-data/family-variants/a9dab2bd-3753-420b-b7b8-a845ac06bfe5.multi.vqsr.filtered.denovo.vep_105.vcf.gz",
      )
    ),
    Datum(
      studyId = "SD_6FPYJQBR",
      pattern = kfStudiesConfiguration("SD_6FPYJQBR").snvVCFPattern,
      paths = Seq("s3://kf-study-us-east-1-prd-sd-6fpyjqbr/harmonized/family-variants/9bacad7b-b55a-4b13-8b92-88c90f47f0e1.CGP.filtered.deNovo.vep.vcf.gz")
    ),
    Datum(
      studyId = "SD_0TYVY1TW",
      pattern = kfStudiesConfiguration("SD_0TYVY1TW").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-0tyvy1tw/harmonized-data/family-variants/9bacad7b-b55a-4b13-8b92-88c90f47f0e1.multi.vqsr.filtered.denovo.vep_105.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_YGVA0E1C",
      pattern = kfStudiesConfiguration("SD_YGVA0E1C").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-ygva0e1c/harmonized/family-variants/9bacad7b-b55a-4b13-8b92-88c90f47f0e1.CGP.filtered.deNovo.vep.vcf.gz",
        "s3://kf-study-us-east-1-prd-sd-ygva0e1c/harmonized/family-variants/9bacad7b-b55a-4b13-8b92-88c90f47f0e1.postCGP.filtered.deNovo.vep.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_W0V965XZ",
      pattern = kfStudiesConfiguration("SD_W0V965XZ").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-w0v965xz/harmonized-data/family-variants/9bacad7b-b55a-4b13-8b92-88c90f47f0e1.CGP.filtered.deNovo.vep.vcf.gz",
      )
    ),
    Datum(
      studyId = "SD_GPZG67FZ",
      pattern = kfStudiesConfiguration("SD_GPZG67FZ").snvVCFPattern,
      paths = Seq(
        "s3://kf-strides-study-us-east-1-prd-sd-gpzg67fz/harmonized-data/family-variants/9bacad7b-b55a-4b13-8b92-88c90f47f0e1.CGP.filtered.deNovo.vep.vcf.gz",
      )
    ),
    Datum(
      studyId = "SD_DZ4GPQX6",
      pattern = kfStudiesConfiguration("SD_DZ4GPQX6").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-dz4gpqx6/harmonized-data/family-variants/9bacad7b-b55a-4b13-8b92-88c90f47f0e1.CGP.filtered.deNovo.vep.vcf.gz",
      )
    ),
    Datum(
      studyId = "SD_JWS3V24D",
      pattern = kfStudiesConfiguration("SD_JWS3V24D").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-jws3v24d/harmonized-data/family-variants/9bacad7b-b55a-4b13-8b92-88c90f47f0e1.postCGP.filtered.deNovo.vep.vcf.gz",
      )
    ),
    Datum(
      studyId = "SD_QBG7P5P7",
      pattern = kfStudiesConfiguration("SD_QBG7P5P7").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-qbg7p5p7/harmonized-data/family-variants/9bacad7b-b55a-4b13-8b92-88c90f47f0e1.CGP.filtered.deNovo.vep.vcf.gz",
      )
    ),
    Datum(
      studyId = "SD_VTTSHWV4",
      pattern = kfStudiesConfiguration("SD_VTTSHWV4").snvVCFPattern,
      paths = Seq("s3://kf-study-us-east-1-prd-sd-vttshwv4/harmonized-data/family-variants/3478bedd-df3d-48e3-bb9b-c0908ce444d1.postCGP.filtered.deNovo.vep.vcf.gz")
    ),
    Datum(
      studyId = "SD_NMVV8A1Y",
      pattern = kfStudiesConfiguration("SD_NMVV8A1Y").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-nmvv8a1y/harmonized-data/family-variants/9cc06c18-62a2-4ca1-990b-0ff991f121f6.CGP.filtered.deNovo.vep.vcf.gz",
        "s3://kf-study-us-east-1-prd-sd-nmvv8a1y/harmonized-data/family-variants/716dfcf7-55cf-40d3-8174-3375b9df3182.postCGP.filtered.deNovo.vep.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_JK4Z4T6V",
      pattern = kfStudiesConfiguration("SD_JK4Z4T6V").snvVCFPattern,
      paths = Seq(
        "s3://kf-strides-study-us-east-1-prd-sd-jk4z4t6v/harmonized-data/family-variants/7faa8c37-3d1a-4e02-8772-b7539accae72.CGP.filtered.deNovo.vep.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_PET7Q6F2",
      pattern = kfStudiesConfiguration("SD_PET7Q6F2").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-pet7q6f2/harmonized-data/simple-variants/a9dab2bd-3753-420b-b7b8-a845ac06bfe5.CGP.filtered.deNovo.vep.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_DYPMEHHF",
      pattern = kfStudiesConfiguration("SD_DYPMEHHF").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-dypmehhf/harmonized-data/family-variants/a9dab2bd-3753-420b-b7b8-a845ac06bfe5.multi.vqsr.filtered.denovo.vep_105.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_P445ACHV",
      pattern = kfStudiesConfiguration("SD_P445ACHV").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-p445achv/harmonized-data/family-variants/a9dab2bd-3753-420b-b7b8-a845ac06bfe5.CGP.filtered.deNovo.vep.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_15A2MQQ9",
      pattern = kfStudiesConfiguration("SD_15A2MQQ9").snvVCFPattern,
      paths = Seq(
        "s3://kf-strides-study-us-east-1-prd-sd-15a2mqq9/harmonized-data/family-variants/447d9f46-9328-4bae-a39d-ac29d8a0c197.multi.vqsr.filtered.denovo.vep_105.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_9PYZAHHE",
      pattern = kfStudiesConfiguration("SD_9PYZAHHE").snvVCFPattern,
      paths = Seq("s3://kf-study-us-east-1-prd-sd-9pyzahhe/harmonized/family-variants/cc357ef5-27b7-4af0-b106-dfc8d932f22b.CGP.filtered.deNovo.vep.vcf.gz")
    ),
    Datum(
      studyId = "SD_R0EPRSGS",
      pattern = kfStudiesConfiguration("SD_R0EPRSGS").snvVCFPattern,
      paths = Seq("s3://kf-study-us-east-1-prd-sd-r0eprsgs/harmonized/family-variants/8ade709c-af25-4e94-aba4-19e1a0511dca.CGP.filtered.deNovo.vep.vcf.gz")
    ),
    Datum(
      studyId = "SD_W6FWTD8A",
      pattern = kfStudiesConfiguration("SD_W6FWTD8A").snvVCFPattern,
      paths = Seq("s3://kf-strides-study-us-east-1-prd-sd-w6fwtd8a/harmonized-data/family-variants/5ec00e64-8aa0-45b6-8012-272647201f21.CGP.filtered.deNovo.vep.vcf.gz")
    ),
    Datum(
      studyId = "SD_ZXJFFMEF",
      pattern = kfStudiesConfiguration("SD_ZXJFFMEF").snvVCFPattern,
      paths = Seq("s3://kf-study-us-east-1-prd-sd-zxjffmef/harmonized-data/simple-variants/c3556efe-6ccd-4a40-85ac-1eab04b7b575.CGP.filtered.deNovo.vep.vcf.gz")
    ),
    Datum(
      studyId = "SD_DZTB5HRR",
      pattern = kfStudiesConfiguration("SD_DZTB5HRR").snvVCFPattern,
      paths = Seq(
        "s3://kf-study-us-east-1-prd-sd-dztb5hrr/harmonized/family-variants/a9dab2bd-3753-420b-b7b8-a845ac06bfe5.CGP.filtered.deNovo.vep.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_AQ9KVN5P",
      pattern = kfStudiesConfiguration("SD_AQ9KVN5P").snvVCFPattern,
      paths = Seq(
        "s3://kf-strides-study-us-east-1-prd-sd-aq9kvn5p/harmonized-data/simple-variants/a9dab2bd-3753-420b-b7b8-a845ac06bfe5.CGP.filtered.deNovo.vep.vcf.gz",
        "s3://kf-strides-study-us-east-1-prd-sd-aq9kvn5p/harmonized-data/simple-variants/a9dab2bd-3753-420b-b7b8-a845ac06bfe5.postCGP.Gfiltered.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_BHJXBDQK",
      pattern = kfStudiesConfiguration("SD_BHJXBDQK").snvVCFPattern,
      paths = Seq(
        "s3://cds-246-phs002517-sequencefiles-p30-fy20/kf-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/a9dab2bd-3753-420b-b7b8-a845ac06bfe5.CGP.filtered.vep.vcf.gz",
        "s3://cds-246-phs002517-sequencefiles-p30-fy20/kf-study-us-east-1-prd-sd-bhjxbdqk/harmonized-data/simple-variants/a9dab2bd-3753-420b-b7b8-a845ac06bfe5.multi.vqsr.filtered.denovo.vep_105.vcf.gz",
        "s3://cds-306-phs002517-x01/harmonized-data/family-variants/a9dab2bd-3753-420b-b7b8-a845ac06bfe5.CGP.filtered.vep.vcf.gz",
        "s3://cds-306-phs002517-x01/harmonized-data/family-variants/a9dab2bd-3753-420b-b7b8-a845ac06bfe5.multi.vqsr.filtered.denovo.vep_105.vcf.gz"
      )
    ),
    Datum(
      studyId = "SD_8Y99QZJJ",
      pattern = kfStudiesConfiguration("SD_8Y99QZJJ").snvVCFPattern,
      paths = Seq(
        "s3://cds-246-phs002517-sequencefiles-p30-fy20/kf-study-us-east-1-prd-sd-8y99qzjj/harmonized-data/simple-variants/a9dab2bd-3753-420b-b7b8-a845ac06bfe5.CGP.filtered.deNovo.vep.vcf.gz",
      )
    ),
  )

  val dataStudyIds: Set[String] = data.map(x => x.studyId).toSet
  // ===== Sanity checks ===== //
  dataStudyIds.size shouldEqual data.length
  data.count(x => {
    x.paths.nonEmpty
  }) shouldEqual data.count(x => x.paths.nonEmpty)
  kfStudiesConfiguration.keys.forall(k => dataStudyIds.contains(k)) shouldBe true
  // ===== //

  behavior of "an Rlike-expression in StudyConfiguration"

  it should "capture desired files for given study" in {
    val df = data
      // Use only studies where real examples were found.
      .filter(x => x.paths.nonEmpty)
      .map(x => {
        val xDf = Seq(x).toDF("study_id", "pattern", "paths")
        xDf
          .select(
            col("study_id"),
            explode(col("paths")) as "path"
          )
          .withColumn(
            "rlike?",
            col("path").rlike(x.pattern)
          )
      }).reduce((x, y) => x.union(y))
    df.select(col("rlike?")).where(col("rlike?")).distinct.count shouldBe 1
  }
}