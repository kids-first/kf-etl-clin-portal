package bio.ferlab.fhir.etl

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, DatasetConf}
import bio.ferlab.datalake.spark3.elasticsearch.{ElasticSearchClient, Indexer}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}


object IndexTask extends App {

  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(
  esNodes,          // http://localhost:9200
  esPort,           // 9200
  release_id,       // release id
  study_ids,        // study ids separated by ;
  jobType,          // study_centric or participant_centric or file_centric or biospecimen_centric
  configFile        // config/qa.conf or config/prod.conf or config/dev.conf
  ) = args

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources(configFile)

  val esConfigs = Map(
    "es.index.auto.create" -> "true",
    "es.net.ssl" -> "true",
    "es.net.ssl.cert.allow.self.signed" -> "true",
    "es.nodes" -> esNodes,
    "es.nodes.wan.only" -> "true",
    "es.wan.only" -> "true",
    "spark.es.nodes.wan.only" -> "true",
    "es.port" -> esPort)

  val sparkConfigs: SparkConf =
    (conf.sparkconf ++ esConfigs)
      .foldLeft(new SparkConf()){ case (c, (k, v)) => c.set(k, v) }

  implicit val spark: SparkSession = SparkSession.builder
    .config(sparkConfigs)
    .enableHiveSupport()
    .appName(s"IndexTask")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val templatePath = s"${conf.storages.find(_.id == "storage").get.path}/templates/template_$jobType.json"

  implicit val esClient: ElasticSearchClient = new ElasticSearchClient(esNodes.split(',').head, None, None)

  val ds: DatasetConf = jobType.toLowerCase match {
    case "study_centric" => conf.getDataset("es_index_study_centric")
    case "participant_centric" => conf.getDataset("es_index_participant_centric")
    case "file_centric" => conf.getDataset("es_index_file_centric")
    case "biospecimen_centric" => conf.getDataset("es_index_biospecimen_centric")
  }

  val studyList = study_ids.split(";")

  studyList.map(studyId => {
    val indexName = s"${jobType}_${studyId}_$release_id".toLowerCase

    println(s"Run Index Task to fill index $indexName")

    val df: DataFrame = ds.read
      .where(col("release_id") === release_id)
      .where(col("study_id") === studyId)

    new Indexer("index", templatePath, indexName)
      .run(df)
  })

}
