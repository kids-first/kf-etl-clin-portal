from add_portal_etl_emr_step import PortalEtlEmrStepService
from portal_emr_step_builder import EmrStepBuilder, EmrStepArgumentBuilder

DEFAULT_GENOMIC_INDEX_PORTAL_ETL_STEPS = ['variant_centric', 'variant_suggestions', 'gene_centric', 'gene_suggestions']

CHROMOSOMES = ['1', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '2', '20', '21', '22', '2', '3', '5',
               '6', '7', '8', '9', 'X', 'Y']


class GenomicIndexPortalEtlEmrStepService(PortalEtlEmrStepService):

    def __init__(self, etl_args: dict):
        super(GenomicIndexPortalEtlEmrStepService, self).__init__(etl_args=etl_args)

    def get_default_etl_steps_to_execute(self) -> list:
        return DEFAULT_GENOMIC_INDEX_PORTAL_ETL_STEPS

    def get_next_steps(self, portal_etl_steps_to_execute: list, current_etl_steps: list, study_ids: list) -> list:
        user_input = self.etl_args['input']
        user_custom_chromosomes = user_input.get('chromosomes', [])

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

        if etl_step_name in ['variant-centric', 'variant-suggestions']:
            next_steps_to_execute = generate_variant_etl_steps(etl_step_name, user_custom_chromosomes)
        else:
            next_steps_to_execute = [(etl_step_name, 'all')]
        return next_steps_to_execute

    def get_etl_step_description(self, next_etl_steps: list) -> list:
        return [self.generate_genomic_index_step_description(portal_etl_job_name, chromosome) for
                (portal_etl_job_name, chromosome) in next_etl_steps]

    def get_etl_current_step_names(self, current_etl_steps) -> list:
        return [etl_step_name if chromosome == 'all' else f"{etl_step_name}-{chromosome}" for
                (etl_step_name, chromosome) in current_etl_steps]

    def generate_genomic_index_step_description(self, portal_etl_step_name: str, chromosome: str):
        elastic_search_endpoint = self.etl_args['esEndpoint']
        user_input = self.etl_args['input']
        release_id = user_input['releaseId']
        sample = user_input.get('sample', 'all')

        # Force Sample to all when step is gene*
        if portal_etl_step_name in ['gene-centric', 'gene-suggestions']:
            sample = 'all'

        bucket = self.etl_args['etlPortalBucket']
        environment = self.etl_args['environment']
        account = self.etl_args['account']
        portal_etl_step_class = 'bio.ferlab.etl.indexed.genomic.RunIndexGenomic'
        return EmrStepBuilder(job_name=f'Index {portal_etl_step_name} - {release_id} - {chromosome}',
                              step_args=EmrStepArgumentBuilder()
                              .with_spark_job(class_name=portal_etl_step_class, etl_portal_bucket=bucket)
                              .with_packages(['org.elasticsearch:elasticsearch-spark-30_2.12:7.17.12'])
                              .with_custom_arg(elastic_search_endpoint)
                              .with_custom_arg('443')
                              .with_release_id(release_id=release_id, include_flag=False)
                              .with_custom_arg(portal_etl_step_name)
                              .with_config(env=environment, account=account, include_flag=False)
                              .with_custom_arg(f"s3a://{bucket}/es_index/{portal_etl_step_name}")
                              .with_custom_arg(chromosome)
                              .with_custom_arg(sample).build()).build()


def generate_variant_etl_steps(etl_step_name: str, custom_chromosomes: list) -> list:
    t = all(c in CHROMOSOMES for c in custom_chromosomes)
    if not custom_chromosomes:
        return [(etl_step_name, chromosome) for chromosome in CHROMOSOMES]
    if custom_chromosomes[0] == 'all':
        return [(etl_step_name, 'all')]
    elif all(c in CHROMOSOMES for c in custom_chromosomes):
        return [(etl_step_name, chromosome) for chromosome in custom_chromosomes]
    return []


if __name__ == '__main__':
    test_custom_chromosomes = ['x', '1', '10']
    print(generate_variant_etl_steps(DEFAULT_GENOMIC_INDEX_PORTAL_ETL_STEPS[0], test_custom_chromosomes))
