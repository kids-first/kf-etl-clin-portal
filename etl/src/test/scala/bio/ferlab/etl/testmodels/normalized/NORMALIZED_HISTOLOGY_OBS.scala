package bio.ferlab.etl.testmodels.normalized


case class NORMALIZED_HISTOLOGY_OBS(
                                     `specimen_id`: String = null,
                                     `condition_id`: String = null,
                                     `patient_id`: String = null,
                                     `source_text_tumor_descriptor`: Option[String] = None,
                                     `study_id`: String = "SD_Z6MWD3H0",
                                     `release_id`: String = "re_000001"
                                   )
