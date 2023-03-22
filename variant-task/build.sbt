val projectName = "variant-task"
val organization = "bio.ferlab"
val version = "1.0"

assembly / mainClass := Some("bio.ferlab.etl.VariantTask")
assembly / assemblyJarName := "variant-task.jar"