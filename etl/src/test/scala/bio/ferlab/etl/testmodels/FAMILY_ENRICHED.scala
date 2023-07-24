package bio.ferlab.etl.testmodels

case class FAMILY_ENRICHED(
                            family_fhir_id: String = "f",
                            relations: Seq[RELATION] = Seq.empty,
                          )

case class RELATION(`participant_id`: String = "p", `role`: String = "father")