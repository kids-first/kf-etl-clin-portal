package model

import java.sql.Timestamp

case class PARTICIPANT_CENTRIC(`participant_id`: String = "PT_48DYT4PP",
                               `study_id`: String = "SD_Z6MWD3H0",
                               `external_id`: String = "PAVKKD",
                               `ethnicity`: String = "Not Reported",
                               `race`: String = "Not Reported",
                               `fhir_id`: String = ".",
                               `gender`: String = "GRCh37",
                               `deceasedBoolean`: Boolean = false,
                               `deceasedDateTime`: Timestamp = Timestamp.valueOf("2020-12-17 13:14:26.581"),
                               `study`: STUDY = STUDY(),
                               `biospecimens`: Seq[BIOSPECIMEN] = null,
                               `families_id`: Seq[String] = null,
                               `families`: Seq[FAMILY] = null
                            )