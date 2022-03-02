import sbtassembly.AssemblyPlugin.autoImport.assembly

lazy val config = (project in file("config"))

lazy val fhavro_export = (project in file("fhavro-export"))

val sparkDeps = Seq(
  "bio.ferlab" %% "datalake-spark31" % "0.2.20",
  "org.apache.spark" %% "spark-sql" % "3.1.2" % Provided,
  "org.apache.spark" %% "spark-hive" % "3.1.2" % Provided,
  "org.apache.hadoop" % "hadoop-client" % "3.2.0" % Provided,
  "org.apache.hadoop" % "hadoop-aws" % "3.2.0" % Provided,
  "io.delta" %% "delta-core" % "1.0.0" % Provided
)

val commonSettings = Seq(
  scalaVersion := "2.12.14",
  Compile / unmanagedResourceDirectories += config.base / "output",
  libraryDependencies ++= sparkDeps ++ Seq(
    "org.scalatest" %% "scalatest" % "3.0.8" % Test
  ),
  assembly / assemblyShadeRules := Seq(
    ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inAll
  ),
  assembly / assemblyMergeStrategy := {
    case "META-INF/io.netty.versions.properties" => MergeStrategy.last
    case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => MergeStrategy.last
    case "META-INF/DISCLAIMER" => MergeStrategy.last
    case "mozilla/public-suffix-list.txt" => MergeStrategy.last
    case "overview.html" => MergeStrategy.last
    case "git.properties" => MergeStrategy.discard
    case "mime.types" => MergeStrategy.first
    case PathList("scala", "annotation", "nowarn.class" | "nowarn$.class") => MergeStrategy.first
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  },
  assembly / test := {}
)

lazy val import_task = (project in file("import-task")).settings(libraryDependencies ++= sparkDeps, commonSettings)

lazy val prepare_index = (project in file("prepare-index")).settings(commonSettings:_*)

lazy val index_task = (project in file("index-task")).settings(commonSettings)

lazy val publish_task = (project in file("publish-task")).settings(commonSettings)