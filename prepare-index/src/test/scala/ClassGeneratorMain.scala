import bio.ferlab.datalake.commons.config.{Configuration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.{LOCAL, S3}
import bio.ferlab.datalake.spark3.{ClassGenerator, etl}
import bio.ferlab.datalake.spark3.loader.GenericLoader.read
import bio.ferlab.fhir.etl.centricTypes.ParticipantCentric

object ClassGeneratorMain extends App with WithSparkSession {


  val output: String = getClass.getClassLoader.getResource(".").getFile

//  implicit val conf: Configuration = Configuration(List(StorageConf("clin", output)))

  //val job = new RawToNormalizedETL(Raw.practitioner, Normalized.practitioner, practitionerMappings)
  //
  //val df = job.transform(job.extract()).where("id='PR00108'")
  //
  //df.show(false)
  //df.printSchema()

  //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "OrganizationOutput", df, "src/test/scala/")
  //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "PartitionerOutput", df, "src/test/scala/")
}
