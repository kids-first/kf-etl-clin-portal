package bio.ferlab.etl.testmodels.normalized

case class NORMALIZED_TASK(
                 `fhir_id`: String = "38734",
                 `task_id`: String = "SE_XQVJFY1A",
                 `study_id`: String = "S1",
                 `release_id`: String = "R1",
                 `document_reference_fhir_ids`: Seq[String] = Seq.empty[String],
                 `biospecimen_fhir_ids`: Seq[String] = Seq.empty[String],
                 `experiment_strategy`: String = "WGS",
                 `instrument_model`: String = "Not Reported",
                 `library_name`: String = "Not Reported",
                 `library_strand`: String = "Not Reported",
                 `platform`: String = "Illumina",
               )


