import sbtassembly.AssemblyPlugin.autoImport.assembly


lazy val fhavro_export = project in file("fhavro-export")

val sparkDepsSetting = Seq(
  libraryDependencies ++= Seq("bio.ferlab" %% "datalake-spark3" % "5.0.1",
    "org.apache.spark" %% "spark-sql" % "3.3.0" % Provided, //emr-6.9.0
    "org.apache.spark" %% "spark-hive" % "3.3.0" % Provided, //emr-6.9.0
    "org.apache.hadoop" % "hadoop-client" % "3.3.3" % Provided, //emr-6.9.0
    "org.apache.hadoop" % "hadoop-aws" % "3.3.3" % Provided, //emr-6.9.0
    "io.delta" %% "delta-core" % "2.1.0" % Provided,
    "org.scalatest" %% "scalatest" % "3.2.9" % Test
  )
)

val commonSettings = Seq(
  scalaVersion := "2.12.14",
  Compile / unmanagedResourceDirectories += config.base / "output",
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
    case "module-info.class" => MergeStrategy.first
    case PathList("scala", "annotation", "nowarn.class" | "nowarn$.class") => MergeStrategy.first
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  },
  assembly / test := {}


)
lazy val config = (project in file("config")).settings(sparkDepsSetting)
lazy val dataservice_export = (project in file("dataservice-export")).dependsOn(config).settings(commonSettings ++ sparkDepsSetting)

lazy val import_task = (project in file("import-task")).dependsOn(config).settings(commonSettings ++ sparkDepsSetting)

lazy val prepare_index = (project in file("prepare-index")).dependsOn(config).settings(commonSettings ++ sparkDepsSetting)

lazy val index_task = (project in file("index-task")).dependsOn(config).settings(commonSettings ++ sparkDepsSetting)

lazy val publish_task = (project in file("publish-task")).dependsOn(config).settings(commonSettings ++ sparkDepsSetting)

lazy val enrich_task = (project in file("enrich")).dependsOn(config).settings(commonSettings ++ sparkDepsSetting)