val projectName = "import-task"
val organization = "bio.ferlab"
val version = "1.0"

ThisBuild / scalaVersion := "2.12.18"
libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-simple" % "1.7.36" % Runtime
)
