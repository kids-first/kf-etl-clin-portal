val projectName = "import-task"
val organization = "bio.ferlab"
val version = "1.0"

assembly / mainClass := Some("bio.ferlab.fhir.etl.ImportTask")
assembly / assemblyJarName := "import-task.jar"