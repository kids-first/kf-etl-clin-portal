package bio.ferlab.etl.testmodels.prepared

case class PREPARED_ONTOLOGY_TERM(
                               `name`: String = "Abnormality of the cardiovascular system (HP:0001626)",
                               `parents`: Seq[String] = Seq.empty,
                               `is_tagged`: Boolean = false,
                               `is_leaf`: Boolean = false,
                               `age_at_event_days`: Seq[Int] = Seq.empty
                             )