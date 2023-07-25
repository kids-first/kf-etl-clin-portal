package bio.ferlab.etl.testmodels.normalized

case class NORMALIZED_VITAL_STATUS(
                                   `fhir_id`: String = "679676",
                                   `participant_fhir_id`: String = "38729",
                                   `vital_status`: String = "Alive",
                                   `observation_id`: String = "OC_ZKP9A89H",
                                   `age_at_event_days`: AGE_AT_EVENT = AGE_AT_EVENT(),
                                   `study_id`: String = "SD_Z6MWD3H0",
                                   `release_id`: String = "re_000001"
                                   )
