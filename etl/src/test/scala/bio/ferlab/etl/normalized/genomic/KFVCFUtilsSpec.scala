package bio.ferlab.etl.normalized.genomic

import bio.ferlab.etl.normalized.genomic.KFVCFUtils.{V1, V2, V2_WITHOUT_PG, calculateVersionFromHeaders}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KFVCFUtilsSpec extends AnyFlatSpec with Matchers{
  "extractBucketNames" should "return a list of tuples with the original string and extracted bucket name" in {
    val inputList = Seq("s3a://bucket1/folder1/file1.vcf", "s3a://bucket2/folder1/file1.vcf")

    val expectedOutput = Seq(
      ("s3a://bucket1/folder1/file1.vcf", "s3a://bucket1"),
      ("s3a://bucket2/folder1/file1.vcf", "s3a://bucket2")
    )

    val outputList = KFVCFUtils.extractBucketNames(inputList)

    outputList shouldEqual expectedOutput
  }

  "calculateVersionFromHeaders" should "return V2_WITHOUT_PG" in {
    val input = Seq(
      "##fileformat=VCFv4.2",
      "##fileDate=2020-10-01",
      "##source=Ensembl VEP 100.2",
      "##reference=GRCh38",
      "##INFO=<ID=DS,Number=1,Type=Float,Description=\"The estimated ALT dose (number of ALT alleles per haploid genome)\">",
      "##INFO=<ID=END,Number=1,Type=Integer,Description=\"End position of the variant described in this record\">",
      "##INFO=<ID=DG,Number=1,Type=String,Description=\"Dosage genotype\">"
    )

    calculateVersionFromHeaders(input.iterator) shouldBe V2_WITHOUT_PG
  }

  it should "return V2" in {
    val input = Seq(
      "##fileformat=VCFv4.2",
      "##fileDate=2020-10-01",
      "##source=Ensembl VEP 100.2",
      "##reference=GRCh38",
      "##INFO=<ID=DS,Number=1,Type=Float,Description=\"The estimated ALT dose (number of ALT alleles per haploid genome)\">",
      "##INFO=<ID=END,Number=1,Type=Integer,Description=\"End position of the variant described in this record\">",
      "##INFO=<ID=PG,Number=1,Type=String,Description=\"Phasing genotype\">",
      "##INFO=<ID=DG,Number=1,Type=String,Description=\"Dosage genotype\">"

    )

    calculateVersionFromHeaders(input.iterator) shouldBe V2
  }

  it should "return V1" in {
    val input = Seq(
      "##fileformat=VCFv4.2",
      "##fileDate=2020-10-01",
      "##source=Ensembl VEP 100.2",
      "##reference=GRCh38",
      "##INFO=<ID=END,Number=1,Type=Integer,Description=\"End position of the variant described in this record\">",
      "##INFO=<ID=PG,Number=1,Type=String,Description=\"Phasing genotype\">",
      "##INFO=<ID=DG,Number=1,Type=String,Description=\"Dosage genotype\">"

    )
    calculateVersionFromHeaders(input.iterator) shouldBe V1
  }


}
