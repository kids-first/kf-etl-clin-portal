package model

case class BIOSPECIMEN_CENTRIC(
                                `fhir_id`: String = "336842",
                                `status`: String = "available",
                                `composition`: String = "Not Reported",
                                `specimen_id`: String = "BS_F6NDMZCN",
                                `external_aliquot_id`: String = null,
                                `method_of_sample_procurement`: String = null,
                                `ncit_id_anatomical_site`: Boolean = false,
                                `participant_fhir_id`: String = "38986",
                                `source_text_anatomical_site`: String = "Not Reported",
                                `uberon_id_anatomical_site`: String = null,
                                `volume_ul`: Long = 11,
                                `volume_ul_unit`: String = null,
                                `study_id`: String = "SD_Z6MWD3H0",
                                `release_id`: String = "re_000001",
                                `study`: LIGHT_STUDY_CENTRIC = LIGHT_STUDY_CENTRIC(),
                                `participant`: SIMPLE_PARTICIPANT = SIMPLE_PARTICIPANT(),
                                `files`: Seq[DOCUMENTREFERENCE_WITH_SEQ_EXP] = Seq.empty
                              )
