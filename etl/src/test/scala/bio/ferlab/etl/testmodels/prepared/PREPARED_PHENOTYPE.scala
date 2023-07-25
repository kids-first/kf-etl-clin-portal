package bio.ferlab.etl.testmodels.prepared

case class PREPARED_PHENOTYPE(
                      `fhir_id`: String = "678509",
                      `hpo_phenotype_observed`: String = "Atrial septal defect (HP:0001631)",
                      `hpo_phenotype_not_observed`: String = null,
                      `is_observed`: Boolean = false,
                      `age_at_event_days`: Int = 0
                    )


