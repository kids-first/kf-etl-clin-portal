from add_portal_etl_emr_step_clinical import ClinicalPortalEtlEmrStepService
from add_portal_etl_emr_step_genomic import GenomicPortalEtlEmrStepService
from genomic_index_portal_emr_step_service import GenomicIndexPortalEtlEmrStepService


def add_portal_etl_emr_step(etl_args, context):
    """
    Adds a new ETL step to the EMR cluster for portal ETL.

    Args:
        etl_args (dict): ETL configuration arguments.
        context: Context information.

    Returns:
        dict: Updated ETL arguments.
    """
    print(f'Add Step to Portal ETL {etl_args}')
    run_genomic_index_etl = etl_args.get('genomicIndexEtl', False)

    if run_genomic_index_etl:
        portal_etl_step_service = GenomicIndexPortalEtlEmrStepService(etl_args)
    else:
        user_input = etl_args['input']
        run_genomic_etl = user_input.get('runGenomicEtl', False)

        portal_etl_step_service = GenomicPortalEtlEmrStepService(
            etl_args) if run_genomic_etl else ClinicalPortalEtlEmrStepService(etl_args)

    updated_args = portal_etl_step_service.submit_next_portal_etl_step()
    return updated_args