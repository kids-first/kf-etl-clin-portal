from typing import List, Any

from portal_emr_step_builder import EmrStepBuilder, EmrStepArgumentBuilder
from add_portal_etl_emr_step import PortalEtlEmrStepService

# Default list of Portal ETL Steps
DEFAULT_PORTAL_ETL_STEPS = ['normalize-snv', 'normalize-consequences', 'enrich-variant', 'enrich-consequences',
                            'prepare-variant_centric',
                            'prepare-variant_suggestions', 'prepare-gene_centric', 'prepare-gene_suggestions']


class GenomicPortalEtlEmrStepService(PortalEtlEmrStepService):
    def __int__(self, etl_args: dict):
        super(GenomicPortalEtlEmrStepService, self).__init__(etl_args)

    def get_default_etl_steps_to_execute(self) -> list:
        return DEFAULT_PORTAL_ETL_STEPS

    def get_next_steps(self, portal_etl_steps_to_execute: list, current_etl_steps: list, study_ids: list):
        """
         Gets the next ETL step in the process.
         Returns:
             str: The next ETL step.
             :param portal_etl_steps_to_execute:
             :param current_etl_steps:
             :param study_ids:
         """
        etl_step_name = ""
        next_steps_to_execute = []
        if not current_etl_steps:
            etl_step_name = portal_etl_steps_to_execute[0]
        else:
            try:
                index = next(
                    (i for i, prefix in enumerate(portal_etl_steps_to_execute) if
                     current_etl_steps[-1].startswith(prefix)),
                    None)

                if index is not None and index < len(portal_etl_steps_to_execute) - 1:
                    etl_step_name = portal_etl_steps_to_execute[index + 1]
                else:
                    return []  # No next step
            except ValueError:
                return []  # Current step not found in the list

        if etl_step_name in ['normalize-snv', 'normalize-consequences']:
            next_steps_to_execute = [(etl_step_name, study_id) for study_id in study_ids]
        elif etl_step_name.startswith('enrich') or etl_step_name.startswith('prepare'):
            prefix = etl_step_name.split('-')[0]
            next_steps_to_execute = [(etl_step, None) for etl_step in portal_etl_steps_to_execute if
                                     etl_step.startswith(prefix)]
        return next_steps_to_execute

    def get_etl_step_description(self, next_etl_steps: list) -> list[Any]:
        return [PORTAL_ETL_STEP_DESCRIPTION_MAP[portal_etl_step_name](etl_config=self.etl_args, study_id=study_id)
                for
                (portal_etl_step_name, study_id) in
                next_etl_steps]

    def get_etl_current_step_names(self, current_etl_steps) -> list:
        return [portal_etl_step_name if study_id is None else f'{portal_etl_step_name}-{study_id}' for
                (portal_etl_step_name, study_id)
                in current_etl_steps]


PORTAL_ETL_STEP_DESCRIPTION_MAP = {
    'normalize-snv': lambda etl_config, study_id:
    EmrStepBuilder(job_name=f"Normalize-snv-{study_id}",
                   step_args=EmrStepArgumentBuilder()
                   .with_spark_job(
                             "bio.ferlab.etl.normalized.genomic.RunNormalizeGenomic", etl_config['etlPortalBucket'])
                   .with_custom_arg("snv")
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=True)
                   .with_steps()
                   .with_custom_args(["--study-id", f"{study_id}"])
                   .with_release_id(etl_config['input']['releaseId'], include_flag=True)
                   .with_custom_args(
                             ["--reference-genome-path", "/mnt/GRCh38_full_analysis_set_plus_decoy_hla.fa"])
                   .build()
                   ).build(),

    'normalize-consequences': lambda etl_config, study_id:
    EmrStepBuilder(job_name=f"Normalize-consequences-{study_id}",
                   step_args=EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.normalized.genomic.RunNormalizeGenomic",
                                         etl_config['etlPortalBucket'])
                   .with_custom_arg("consequences")
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=True)
                   .with_steps()
                   .with_custom_args(["--study-id", f"{study_id}"])
                   .with_custom_args(
                             ["--reference-genome-path", "/mnt/GRCh38_full_analysis_set_plus_decoy_hla.fa"])
                   .build()
                   ).build(),

    'enrich-variant': lambda etl_config, study_id:
    EmrStepBuilder(job_name="Enrich-snv",
                   step_args=EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.enriched.genomic.RunEnrichGenomic",
                                         etl_config['etlPortalBucket'])
                   .with_custom_arg("snv")
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=True)
                   .with_steps()
                   .build()
                   ).build(),

    'enrich-consequences': lambda etl_config, study_id:
    EmrStepBuilder(job_name="Enrich-consequences",
                   step_args=EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.enriched.genomic.RunEnrichGenomic",
                                         etl_config['etlPortalBucket'])
                   .with_custom_arg("consequences")
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=True)
                   .with_steps()
                   .build()
                   ).build(),

    'prepare-variant_centric': lambda etl_config, study_id:
    EmrStepBuilder(job_name="Prepare-variant_centric",
                   step_args=EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.prepared.genomic.RunPrepareGenomic",
                                         etl_config['etlPortalBucket'])
                   .with_custom_arg("variant_centric")
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=True)
                   .with_steps()
                   .build()
                   ).build(),

    'prepare-variant_suggestions': lambda etl_config, study_id:
    EmrStepBuilder(job_name="Prepare-variant_suggestions",
                   step_args=EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.prepared.genomic.RunPrepareGenomic",
                                         etl_config['etlPortalBucket'])
                   .with_custom_arg("variant_suggestions")
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=True)
                   .with_steps()
                   .build()
                   ).build(),

    'prepare-gene_centric': lambda etl_config, study_id:
    EmrStepBuilder(job_name="Prepare-gene_centric",
                   step_args=EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.prepared.genomic.RunPrepareGenomic",
                                         etl_config['etlPortalBucket'])
                   .with_custom_arg("gene_centric")
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=True)
                   .with_steps()
                   .build()
                   ).build(),

    'prepare-gene_suggestions': lambda etl_config, study_id:
    EmrStepBuilder(job_name="Prepare-gene_suggestions",
                   step_args=EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.prepared.genomic.RunPrepareGenomic",
                                         etl_config['etlPortalBucket'])
                   .with_custom_arg("gene_centric")
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=True)
                   .with_steps()
                   .build()
                   ).build(),
}

if __name__ == '__main__':
    test1 = 'Cleanup jars'
    test2 = 'Download and Run Fhavro-export'
    test3 = 'Prepare Index'
    test4 = 'Index Biospecimen'

    test_etl_config = {
        'environment': 'qa',
        'releaseId': 're_004',
        'etlVariantBucket': 'kf-strides-232196027141-datalake-qa',
        'instanceCount': 1,
        'clusterSize': 'large',
        'etlPortalBucket': 'bucket',
        'account': 'kf-strides',
        'input': {
            'test': 'test',
            'studyIds': ['SDTest1', 'SDTest2'],
            'releaseId': 're_1'
        },
        'portalEtlClusterId': '1234'
    }

    genomic_step_service = GenomicPortalEtlEmrStepService(test_etl_config)
    args = genomic_step_service.submit_next_portal_etl_step()
    print(args)
    print(GenomicPortalEtlEmrStepService(args).submit_next_portal_etl_step())
