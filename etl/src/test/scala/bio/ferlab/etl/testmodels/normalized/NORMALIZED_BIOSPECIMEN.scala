package bio.ferlab.etl.testmodels.normalized

case class NORMALIZED_BIOSPECIMEN(
                                   `fhir_id`: String = "336842",
                                   `status`: String = "available",
                                   `composition`: String = "Not Reported",
                                   `specimen_id`: String = "BS_F6NDMZCN",
                                   `participant_fhir_id`: String = "38986",
                                   `volume`: Long = 11,
                                   `volume_unit`: String = null,
                                   container_id: Option[String] = None,
                                   `sample_id`: String = "BS_F6NDMZCN",
                                   consent_type: Option[String] = None,
                                   `study_id`: String = "SD_Z6MWD3H0",
                                   `release_id`: String = "re_000001"
                      )
