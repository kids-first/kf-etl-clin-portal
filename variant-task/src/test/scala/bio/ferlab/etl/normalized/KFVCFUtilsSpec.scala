package bio.ferlab.etl.normalized

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
}
