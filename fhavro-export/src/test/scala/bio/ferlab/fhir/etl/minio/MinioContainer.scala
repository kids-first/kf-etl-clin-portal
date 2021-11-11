package bio.ferlab.fhir.etl.minio

import bio.ferlab.fhir.etl.IContainer
import com.dimafeng.testcontainers.GenericContainer

case object MinioContainer extends IContainer {
  val name = "clin-pipeline-minio-test"
  val port = 9000

  val container: GenericContainer = GenericContainer(
    "minio/minio",
    command = Seq("server", "/data"),
    exposedPorts = Seq(port),
    labels = Map("name" -> name),
    env = Map(
      "AWS_ACCESS_KEY" -> "minioadmin",
      "AWS_SECRET_KEY" -> "minioadmin",
      "AWS_REGION" -> "us-east-1"
    )
  )
}
