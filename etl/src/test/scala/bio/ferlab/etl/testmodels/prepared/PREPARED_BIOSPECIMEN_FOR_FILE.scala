package bio.ferlab.etl.testmodels.prepared

case class PREPARED_BIOSPECIMEN_FOR_FILE(
                                          `fhir_id`: String = "336842",
                                          `biospecimen_facet_ids`: BIOSPECIMEN_FACET_IDS = BIOSPECIMEN_FACET_IDS(),
                                          `status`: String = "available",
                                          `composition`: String = "Not Reported",
                                          `specimen_id`: String = "BS_F6NDMZCN",
                                          `participant_fhir_id`: String = "38986",
                                          `volume`: Long = 11,
                                          `volume_unit`: String = null,
                                          `container_id`: Option[String] = None,
                                          `study_id`: String = "SD_Z6MWD3H0",
                                          `release_id`: String = "re_000001",
                                          `diagnosis_mondo`: Option[String] = None,
                                          `diagnosis_ncit`: Option[String] = None,
                                          `diagnosis_icd`: Seq[String] = null,
                                          `source_text`: Option[String] = None,
                                          `source_text_tumor_location`: Seq[String] = null,
                      )

case class BIOSPECIMEN_FACET_IDS(
                                 biospecimen_fhir_id_1: String = "336842",
                                 biospecimen_fhir_id_2: String = "336842"
                               )