package bio.ferlab.fhir.etl.minio

import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, DeleteObjectRequest, ListObjectsRequest, PutObjectRequest}
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.io.File
import java.net.URI
import scala.collection.JavaConverters._
import scala.util.Random

trait MinioServer {

  val minioPort: Int = MinioContainer.startIfNotRunning()
  val minioEndpoint = s"http://localhost:$minioPort"

  implicit val s3Client: S3Client =
    S3Client.builder()
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("minioadmin", "minioadmin")))
      .endpointOverride(URI.create(minioEndpoint))
      .region(Region.US_EAST_1)
      .serviceConfiguration(S3Configuration.builder()
        .pathStyleAccessEnabled(true)
        .build())
      .httpClient(ApacheHttpClient.create())
      .build()


  val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  val inputBucket = s"input"

  createBuckets()

  def withS3Objects[T](block: String => T): Unit = {
    val inputPrefix = s"run_${Random.nextInt(10000)}"
    LOGGER.info(s"Use input prefix $inputPrefix : $minioEndpoint/minio/$inputBucket/$inputPrefix")
    try {
      block(inputPrefix)
    } finally {
      deleteRecursively(inputBucket, inputPrefix)
    }
  }

  def uploadFileResource(resource: String): Unit = {
    val file = new File(getClass.getResource(s"/$resource").toURI)
    val put = PutObjectRequest.builder()
      .bucket(inputBucket)
      .key(file.getName)
      .build()
    s3Client.putObject(put, RequestBody.fromFile(file))
  }

  private def createBuckets(): Unit = {
    val alreadyExistingBuckets = s3Client.listBuckets().buckets().asScala.collect { case b if b.name() == inputBucket => b.name() }
    val bucketsToCreate = Seq(inputBucket).diff(alreadyExistingBuckets)
    bucketsToCreate.foreach { b =>
      val bucketRequest = CreateBucketRequest.builder()
        .bucket(b)
        .build()
      s3Client.createBucket(bucketRequest)
    }
  }

  private def deleteRecursively(bucket: String, prefix: String): Unit = {
    val lsRequest = ListObjectsRequest.builder().bucket(bucket).prefix(prefix).build()
    s3Client.listObjects(lsRequest).contents().asScala.foreach { o =>
      val del = DeleteObjectRequest.builder().bucket(bucket).key(prefix).build()
      s3Client.deleteObject(del)
    }
  }
}
