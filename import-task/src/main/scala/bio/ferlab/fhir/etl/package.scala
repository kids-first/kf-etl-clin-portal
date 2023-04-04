package bio.ferlab.fhir

package object etl {
  val SYS_NCBI_URL = "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin"
  val ROLE_CODE_URL = Seq("http://terminology.hl7.org/CodeSystem")
  val SYS_DATA_TYPES = "https://includedcc.org/fhir/code-systems/data_types"
  val SYS_EXP_STRATEGY = "https://includedcc.org/fhir/code-systems/experimental_strategies"
  val SYS_DATA_CATEGORIES = "https://includedcc.org/fhir/code-systems/data_categories"
  val SYS_PROGRAMS_INCLUDE = "https://includedcc.org/fhir/code-systems/programs"
  val SYS_PROGRAMS_KF = "https://kf-api-dataservice.kidsfirstdrc.org/studies?program="
  val SYS_DATA_ACCESS_TYPES = "https://includedcc.org/fhir/code-systems/data_access_types"
  val SYS_YES_NO = "http://terminology.hl7.org/CodeSystem/v2-0136"
  val SYS_SHORT_CODE_KF = "https://kf-api-dataservice.kidsfirstdrc.org/studies?short_code="
}
