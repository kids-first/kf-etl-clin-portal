package bio.ferlab.etl.testmodels.enriched

import bio.ferlab.etl.testmodels.normalized.AGE_AT_EVENT

case class ENRICHED_HISTOLOGY_DISEASE(
                                           `specimen_id`: String = "336842",
                                           `diagnosis_mondo`: String =  "MONDO:0005072",
                                           `diagnosis_ncit`: String = "NCIT:0005072",
                                           `diagnosis_icd`: Seq[String] = Seq.empty,
                                           `source_text`: String = "Neuroblastoma",
                                           `source_text_tumor_location`: Seq[String] = Seq("Reported Unknown"),
                                           `study_id`: String = "SD_Z6MWD3H0",
                                           `release_id`: String = "re_000001",
                                           `source_text_tumor_descriptor`: Option[String] = None,
                                           `age_at_event`: AGE_AT_EVENT = AGE_AT_EVENT(),
                                         )
