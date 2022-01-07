package model

import java.sql.Timestamp

case class BIOSPECIMEN(
                        `status`: String = "available",
                        `receivedTime`: Timestamp =  null,
                        `fhir_id`: String = "336842",
                        `participant_fhir_id`: String = "38986",
                        `specimen_id`: String = "BS_F6NDMZCN",
                        `composition`: String = "Not Reported",
                        `source_text_anatomical_site`: String = "Not Reported",
                        `ncit_id_anatomical_site`: Boolean = false,
                        `uberon_id_anatomical_site`: String = null,
                        `volume_ul`: Long = 11,
                        `volume_ul_unit`: String = null
                      )