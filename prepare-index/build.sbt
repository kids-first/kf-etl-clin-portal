val projectName = "prepare-index"
val organization = "bio.ferlab"
val version = "1.0"

assembly / mainClass := Some("bio.ferlab.fhir.etl.PrepareIndex")
assembly / assemblyJarName := "prepare-index.jar"