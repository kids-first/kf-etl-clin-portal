package bio.ferlab.etl.testmodels.enriched

case class ENRICHED_FAMILY(
                            family_fhir_id: String = "f",
                            participant_fhir_id: String = "pfi",
                            relations: Seq[RELATION] = Seq.empty,
                          )

case class RELATION(`participant_id`: String = "p", `role`: String = "father")

