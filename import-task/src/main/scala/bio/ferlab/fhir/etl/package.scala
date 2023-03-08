package bio.ferlab.fhir

package object etl {
  //SYSTEM URL
  val SYSTEM_URL_INCLUDE = "https://include.org/htp/fhir"
  val SYSTEM_URL_KF = "https://kf-api-dataservice.kidsfirstdrc.org"
  val SYSTEM_URL = Seq(SYSTEM_URL_INCLUDE, SYSTEM_URL_KF)

  //ResearchStudy

  val SYS_NCBI_URL = "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin"

  //Patient
  val SYS_DATASERVICE_URL = Seq(s"$SYSTEM_URL_KF/participants", s"$SYSTEM_URL_INCLUDE/patient")
  val SYS_US_CORE_RACE_URL = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"
  val SYS_US_CORE_ETHNICITY_URL = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"

  //Observation
  //TODO same for include and KF?
  val ROLE_CODE_URL = Seq("http://terminology.hl7.org/CodeSystem")

  val URN_UNIQUE_ID = "urn:kids-first:unique-string"

  val SYS_DATA_TYPES = "https://includedcc.org/fhir/code-systems/data_types"
  val SYS_EXP_STRATEGY = "https://includedcc.org/fhir/code-systems/experimental_strategies"
  val SYS_DATA_CATEGORIES = "https://includedcc.org/fhir/code-systems/data_categories"
  val SYS_PROGRAMS = "https://includedcc.org/fhir/code-systems/programs"
  val SYS_DATA_ACCESS_TYPES = "https://includedcc.org/fhir/code-systems/data_access_types"
  val SYS_CONSENT_CODE = "https://kf-api-dataservice.kidsfirstdrc.org/biospecimens?dbgap_consent_code="
  val SYS_YES_NO = "http://terminology.hl7.org/CodeSystem/v2-0136"

}
