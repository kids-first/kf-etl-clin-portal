package bio.ferlab.fhir

package object etl {
  //SYSTEM URL

  //ResearchStudy
  val SYS_DATASERVICE_URL = "https://kf-api-dataservice.kidsfirstdrc.org/"
  val SYS_NCBI_URL = "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id="


  //Patient
  val SYS_US_CORE_RACE_URL = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"
  val SYS_US_CORE_ETHNICITY_URL = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"

  //Observation
  val ROLE_CODE_URL = "http://terminology.hl7.org/CodeSystem/v3-RoleCode"

  val URN_UNIQUE_ID = "urn:kids-first:unique-string"

}
