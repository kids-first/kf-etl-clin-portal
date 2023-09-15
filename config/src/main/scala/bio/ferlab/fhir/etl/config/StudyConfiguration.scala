package bio.ferlab.fhir.etl.config

import bio.ferlab.fhir.etl.config.ConfigurationGenerator.pKfStrides

case class StudyConfiguration(snvVCFPattern: String)

object StudyConfiguration {
  type StudiesConfiguration = Map[String, StudyConfiguration]

  def studiesConfigurations(project: String): StudiesConfiguration = if (project == pKfStrides) kfStudiesConfiguration else Map.empty[String, StudyConfiguration]

  val defaultStudyConfiguration: StudyConfiguration = StudyConfiguration(
    snvVCFPattern = "*/harmonized-data/family-variants/.*filtered.deNovo.vep.vcf.gz"
  )

  private val kfStudiesConfiguration = Map(
    "SD_BHJXBDQK" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized-data/simple-variants/.*filtered.*vep.vcf.gz"
    ),
    "SD_ZXJFFMEF" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized-data/simple-variants/.*filtered.deNovo.vep.vcf.gz"
    )

  )
}
