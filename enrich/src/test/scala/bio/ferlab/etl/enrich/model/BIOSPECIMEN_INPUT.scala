package bio.ferlab.etl.enrich.model

case class BIOSPECIMEN_INPUT(
                              `fhir_id`: String = "336842",
                              `sample_id`: String = "BS_F6NDMZCN",
                              `participant_fhir_id`: String = "38986",
                              consent_type: Option[String] = None,
                              `study_id`: String = "SD_Z6MWD3H0",
                              `release_id`: String = "re_000001"
                            )
