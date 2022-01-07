package bio.ferlab.fhir.etl.minio

import org.scalatest.{BeforeAndAfterAll, TestSuite}

trait MinioServerSuite extends MinioServer with TestSuite with BeforeAndAfterAll {}