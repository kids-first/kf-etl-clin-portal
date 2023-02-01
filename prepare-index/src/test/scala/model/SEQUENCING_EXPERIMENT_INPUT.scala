package model

case class SEQUENCING_EXPERIMENT_INPUT(
                                        kf_id: String = "SE_1H1QH9CM",
                                        created_at: Option[String] = None,
                                        modified_at: Option[String] = None,
                                        experiment_date: Option[String] = None,
                                        experiment_strategy: String = "WGS",
                                        center: Option[String] = None,
                                        library_name: Option[String] = None,
                                        library_prep: Option[String] = None,
                                        library_selection: Option[String] = None,
                                        library_strand: Option[String] = None,
                                        is_paired_end: Option[Boolean] = None,
                                        platform: Option[String] = None,
                                        instrument_model: Option[String] = None,
                                        max_insert_size: Option[Long] = None,
                                        mean_insert_size: Option[Double] = None,
                                        mean_depth: Option[Double] = None,
                                        total_reads: Option[Long] = None,
                                        mean_read_length: Option[Double] = None,
                                        external_id: Option[String] = None,
                                        genomic_files: Seq[String] = Nil,
                                        sequencing_center_id: Option[String] = None,
                                        visible: Option[Boolean] = None)

case class SEQUENCING_EXPERIMENT_GENOMIC_FILE_INPUT(created_at: Option[String] = None,
                                                    modified_at: Option[String] = None,
                                                    visible: Option[Boolean] = None,
                                                    external_id: Option[String] = None,
                                                    genomic_file: String = "GF_067MR115",
                                                    kf_id: String = "SG_F5YW7JRQ",
                                                    sequencing_experiment: String = "SE_1H1QH9CM")
