package model

case class SEQUENCING_EXPERIMENT(
                 `fhir_id`: String = "474726",
                 `task_id`: String = "SE_XQVJFY1A",
                 `document_reference_fhir_ids`: Seq[String] = Seq.empty[String],
                 `biospecimen_fhir_ids`: Seq[String] = Seq.empty[String],
                 `experiment_strategy`: String = "WGS",
                 `instrument_model`: String = "Not Reported",
                 `library_name`: String = "Not Reported",
                 `library_strand`: String = "Not Reported",
                 `platform`: String = "Illumina"
               )
