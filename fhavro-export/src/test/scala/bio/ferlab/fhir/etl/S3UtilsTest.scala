package bio.ferlab.fhir.etl

import bio.ferlab.fhir.etl.config.FhirRequest
import bio.ferlab.fhir.etl.minio.MinioServerSuite
import bio.ferlab.fhir.etl.s3.S3Utils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.s3.model.GetObjectRequest

import java.io.File

class S3UtilsTest extends AnyFlatSpec with MinioServerSuite with Matchers {

  "writeContent" should "write file content in s3" in {
    withS3Objects { bucket =>
      val sourceFile = new File(getClass.getResource("/hello-world.txt").getPath)
      val path = s"$bucket/hello-world.txt"
      S3Utils.writeFile(inputBucket, s"$bucket/hello-world.txt", sourceFile)

      val objectRequest = GetObjectRequest
        .builder()
        .key(path)
        .bucket(inputBucket)
        .build()
      val result = new String(s3Client.getObject(objectRequest).readAllBytes())

      result shouldBe "hello world!"
    }
  }

  "exists" should "return false if file does not exist" in {
    withS3Objects { _ =>
      S3Utils.exists(inputBucket, "folder/does_not_exist.txt") shouldBe false
    }
  }

  it should "return true if file does not exist" in {
    withS3Objects { _ =>
      uploadFileResource("hello-world.txt")
      S3Utils.exists(inputBucket, "hello-world.txt") shouldBe true
    }
  }

  "buildKey" should "build a formatted key based on a request" in {
    val fhirRequest = FhirRequest("Patient", "kfdrc-patient", None, None, None, None, None)
    S3Utils.buildKey(fhirRequest, "re_001", "SD_ABC") shouldBe s"fhir/patient/study_id=SD_ABC/release_id=re_001/kfdrc-patient.avro"
  }

  it should "build a formatted key based on a request entity-type" in {
    val fhirRequest = FhirRequest("Patient", "kfdrc-patient", None, None, Some("this_is_patient"), None, None)
    S3Utils.buildKey(fhirRequest, "re_001", "SD_ABC") shouldBe s"fhir/this_is_patient/study_id=SD_ABC/release_id=re_001/kfdrc-patient.avro"
  }

}
