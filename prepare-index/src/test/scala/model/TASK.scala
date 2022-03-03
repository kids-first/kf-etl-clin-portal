package model

case class TASK(
                 `fhir_id`: String = "38734",
                 `task_id`: String = "task1",
                 `study_id`: String = "S1",
                 `release_id`: String = "R1",
                 `document_reference_fhir_ids`: Seq[String] = Seq.empty[String],
                 `biospecimen_fhir_ids`: Seq[String] = Seq.empty[String],
                 `experiment_strategy`: String = "experiment_strategy",
                 `instrument_model`: String = "instrument_model",
                 `library_name`: String = "library_name",
                 `library_strand`: String = "library_strand",
                 `platform`: String = "platform",
               )


