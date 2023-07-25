package bio.ferlab.etl.testmodels.normalized

case class NORMALIZED_ONTOLOGY_TERM(
                           `id`: String = "HP:0001631",
                           `name`: String = "Acute lymphoblastic leukemia (HP:0001631)",
                           `parents`: Seq[String] = Nil,
                           `ancestors`: Seq[TERM] = Nil,
                           `is_leaf`: Boolean = false,
                         )

case class TERM (
                  `id`: String = "HP:0003333",
                  `name`: String = "TOTO",
                  `parents`: Seq[String] = Nil,
                )
