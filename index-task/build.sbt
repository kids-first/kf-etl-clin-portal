val projectName = "index-task"
val organization = "bio.ferlab"
val version = "1.0"

ThisBuild / scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "bio.ferlab" %% "datalake-spark31" % "0.2.8",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.spark" %% "spark-hive" % "3.1.2",
  "org.apache.hadoop" % "hadoop-client" % "3.2.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.2.0",
  "io.delta" %% "delta-core" % "1.0.0",
  "org.elasticsearch" %% "elasticsearch-spark-30" % "7.16.3",

  // Scala module 2.10.0 requires Jackson Databind version >= 2.10.0 and < 2.11.0
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.2",

  // TEST
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  "Sonatype OSS Releases" at "https://s01.oss.sonatype.org/content/repositories/snapshots/",
  "aws-glue-etl-artifacts" at "https://aws-glue-etl-artifacts.s3.amazonaws.com/release/"
)

assembly / assemblyMergeStrategy:= {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assembly / test := {}
assembly / mainClass := Some("bio.ferlab.fhir.etl.IndexTask")
assembly / assemblyJarName := "index-task.jar"