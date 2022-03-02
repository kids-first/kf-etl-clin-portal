val projectName = "publish-task"
val organization = "bio.ferlab"
val version = "1.0"

assembly / mainClass := Some("bio.ferlab.fhir.etl.PublishTask")
assembly / assemblyJarName := "publish-task.jar"