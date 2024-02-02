package bio.ferlab.fhir.etl.config

import bio.ferlab.fhir.etl.config.ConfigurationGenerator.pKfStrides

case class StudyConfiguration(snvVCFPattern: String)

object StudyConfiguration {
  type StudiesConfiguration = Map[String, StudyConfiguration]

  def studiesConfigurations(project: String): StudiesConfiguration = if (project == pKfStrides) kfStudiesConfiguration else Map.empty[String, StudyConfiguration]

  val defaultStudyConfiguration: StudyConfiguration = StudyConfiguration(
    snvVCFPattern = ".*/harmonized-data/family-variants/.*filtered.deNovo.vep.vcf.gz"
  )

  val kfStudiesConfiguration: Map[String, StudyConfiguration] = Map(
    "SD_RM8AFW0R" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz"
    ),
    "SD_Z0D9N23X" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz"
    ),
    "SD_46SK55A3" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz|.*/harmonized/family-variants/.*.postCGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_2CEKQ05V" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.postCGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_54G4WG4R" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz|.*/harmonized-data/family-variants/.*.multi.vqsr.filtered.denovo.vep_105.vcf.gz)"
    ),
    "SD_PREASA7S" -> StudyConfiguration(
      snvVCFPattern = "(" +
        ".*/harmonized/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz" + "|" +
        ".*/harmonized/family-variants/.*.postCGP.filtered.deNovo.vep.vcf.gz" + "|" +
        ".*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz" + "|" +
        ".*/harmonized-data/family-variants/.*.postCGP.filtered.deNovo.vep.vcf.gz" +
        ")"
    ),
    "SD_Z6MWD3H0" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz|.*/harmonized-data/family-variants/.*.multi.vqsr.filtered.denovo.vep_105.vcf.gz)"
    ),
    "SD_6FPYJQBR" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_0TYVY1TW" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_YGVA0E1C" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz|.*/harmonized/family-variants/.*.postCGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_W0V965XZ" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_GPZG67FZ" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_DZ4GPQX6" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_JWS3V24D" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.postCGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_QBG7P5P7" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_VTTSHWV4" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_NMVV8A1Y" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz|.*/harmonized-data/family-variants/.*.postCGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_JK4Z4T6V" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_PET7Q6F2" -> StudyConfiguration(
      snvVCFPattern = ".*/harmonized-data/simple-variants/.*filtered.deNovo.vep.vcf.gz"
    ),
    "SD_DYPMEHHF" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.multi.vqsr.filtered.denovo.vep_105.vcf.gz)"
    ),
    "SD_P445ACHV" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_15A2MQQ9" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_9PYZAHHE" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_R0EPRSGS" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_W6FWTD8A" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_ZXJFFMEF" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/simple-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_DZTB5HRR" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized/family-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    ),
    "SD_AQ9KVN5P" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/simple-variants/.*.CGP.filtered.deNovo.vep.vcf.gz|.*/harmonized-data/simple-variants/.*.postCGP.Gfiltered.vcf.gz)"
    ),
    "SD_BHJXBDQK" -> StudyConfiguration(
      snvVCFPattern = "(" +
        ".*/harmonized-data/simple-variants/.*.CGP.filtered.vep.vcf.gz" + "|" +
        ".*/harmonized-data/family-variants/.*.CGP.filtered.vep.vcf.gz" + "|" +
        ".*/harmonized-data/simple-variants/.*.multi.vqsr.filtered.denovo.vep_105.vcf.gz" +
        ".*/harmonized-data/family-variants/.*.multi.vqsr.filtered.denovo.vep_105.vcf.gz" +
        ")"
    ),
    "SD_8Y99QZJJ" -> StudyConfiguration(
      snvVCFPattern = "(.*/harmonized-data/simple-variants/.*.CGP.filtered.deNovo.vep.vcf.gz)"
    )
  )
}
