package bio.ferlab.etl.testmodels.prepared

import bio.ferlab.etl.testmodels.normalized

case class PREPARED_OUTCOME(
                    `fhir_id`: String = "679676",
                    `participant_fhir_id`: String = "38729",
                    `vital_status`: String = "Alive",
                    `observation_id`: String = "OC_ZKP9A89H",
                    `age_at_event_days`: normalized.AGE_AT_EVENT = normalized.AGE_AT_EVENT()
                  )
