val projectName = "prepare-index"
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

  // Scala module 2.10.0 requires Jackson Databind version >= 2.10.0 and < 2.11.0
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.2",

  // TEST
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

assembly / assemblyMergeStrategy:= {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assembly / test := {}
assembly / mainClass := Some("bio.ferlab.fhir.etl.PrepareIndex")
assembly / assemblyJarName := "prepare-index.jar"