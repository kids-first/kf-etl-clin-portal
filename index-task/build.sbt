val projectName = "index-task"
val organization = "bio.ferlab"
val version = "1.0"

libraryDependencies ++= Seq(
  "org.elasticsearch" %% "elasticsearch-spark-30" % "7.16.3"
)

assembly / mainClass := Some("bio.ferlab.fhir.etl.IndexTask")
assembly / assemblyJarName := "index-task.jar"