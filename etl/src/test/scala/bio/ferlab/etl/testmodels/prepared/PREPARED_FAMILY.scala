package bio.ferlab.etl.testmodels.prepared

import bio.ferlab.etl.testmodels.enriched.RELATION

case class PREPARED_FAMILY(
                            family_id: String = "f",
                            relations_to_proband: Seq[RELATION] = Seq.empty,
                          )
