package bio.ferlab.etl.testmodels.enriched

case class ENRICHED_HISTOLOGY_DISEASE(
                                           `specimen_id`: String = "336842",
                                           `diagnosis_mondo`: String =  "MONDO:0005072",
                                           `diagnosis_ncit`: String = "NCIT:0005072",
                                           `diagnosis_icd`: Seq[String] = Seq.empty,
                                           `source_text`: String = "Neuroblastoma",
                                           `source_text_tumor_location`: Seq[String] = Seq("Reported Unknown"),
                                           `study_id`: String = "SD_Z6MWD3H0",
                                           `release_id`: String = "re_000001"
                                         )
