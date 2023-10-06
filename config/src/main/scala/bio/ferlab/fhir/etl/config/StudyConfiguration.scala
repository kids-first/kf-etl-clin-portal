package bio.ferlab.fhir.etl.config

import bio.ferlab.fhir.etl.config.ConfigurationGenerator.pKfStrides

case class StudyConfiguration(snvVCFPattern: String)

object StudyConfiguration {
  type StudiesConfiguration = Map[String, StudyConfiguration]

  def studiesConfigurations(project: String): StudiesConfiguration = if (project == pKfStrides) kfStudiesConfiguration else Map.empty[String, StudyConfiguration]

  val defaultStudyConfiguration: StudyConfiguration = StudyConfiguration(
    snvVCFPattern = ".*/harmonized-data/family-variants/.*filtered.deNovo.vep.vcf.gz"
  )

  private val kfStudiesConfiguration = Map(
    "SD_46SK55A3" ->StudyConfiguration(
      snvVCFPattern = ".*/harmonized/family-variants/.*filtered.deNovo.vep.vcf.gz"
    ),
    "SD_9PYZAHHE" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized/family-variants/.*filtered.deNovo.vep.vcf.gz"
    ),
    "SD_DYPMEHHF" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized/family-variants/.*filtered.deNovo.vep.vcf.gz"
    ),
    "SD_6FPYJQBR" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized/family-variants/.*filtered.deNovo.vep.vcf.gz"
    ),
    "SD_YGVA0E1C" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized/family-variants/.*filtered.deNovo.vep.vcf.gz"
    ),
    "SD_R0EPRSGS" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized/family-variants/.*filtered.deNovo.vep.vcf.gz"
    ),
    "SD_B8X3C1MX" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized/family-variants/.*filtered.deNovo.vep.vcf.gz"
    ),
    "SD_DK0KRWK8" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized/family-variants/.*filtered.deNovo.vep.vcf.gz"
    ),
    "SD_PET7Q6F2" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized/simple-variants/.*filtered.deNovo.vep.vcf.gz"
    ),
    "SD_BHJXBDQK" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized-data/simple-variants/.*filtered.*vep.vcf.gz"
    ),
    "SD_ZXJFFMEF" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized-data/simple-variants/.*filtered.deNovo.vep.vcf.gz"
    )

  )
}
