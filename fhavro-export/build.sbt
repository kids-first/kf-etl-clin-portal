name := "fhavro-export"
version := "0.0.1"

scalaVersion := "2.13.7"

val awsVersion = "2.16.66"
val fhirVersion = "5.0.2"
val slf4jVersion = "1.7.30"
val avroVersion = "1.10.2"
val fhavroVersion = "0.0.10-SNAPSHOT"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3" % awsVersion,
  "software.amazon.awssdk" % "apache-client" % awsVersion,
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "org.slf4j" % "slf4j-simple" % slf4jVersion,
  "ca.uhn.hapi.fhir" % "hapi-fhir-client" % fhirVersion,
  "ca.uhn.hapi.fhir" % "hapi-fhir-structures-r4" % fhirVersion,
  "ca.uhn.hapi.fhir" % "org.hl7.fhir.r4" % "5.0.0",
  "org.typelevel" %% "cats-core" % "2.3.1",
  "com.typesafe.play" %% "play-json" % "2.9.2",
  "com.github.pureconfig" %% "pureconfig" % "0.15.0",
  "com.softwaremill.sttp.client3" %% "core" % "3.1.0",
  "org.apache.avro" % "avro" % avroVersion,
  "org.apache.avro" % "avro-ipc-netty" % avroVersion,
  "bio.ferlab" % "fhavro" % fhavroVersion,

  // TEST
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.38.8" % "test",
  "org.testcontainers" % "localstack" % "1.15.2" % "test"
)

Test / fork := true

resolvers ++= Seq("Sonatype OSS Releases" at "https://s01.oss.sonatype.org/content/repositories/snapshots/")

assembly / assemblyMergeStrategy:= {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assembly / test := {}
assembly / mainClass := Some("bio.ferlab.fhir.etl.FhavroExport")
assembly / assemblyJarName:= "fhavro-export-etl.jar"