import add_portal_etl_emr_step_clinical
import add_portal_etl_emr_step_genomic

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

    user_input = etl_args['input']
    run_genomic_etl = user_input.get('runGenomicEtl', False)

    if run_genomic_etl:
        etl_args = add_portal_etl_emr_step_genomic.add_portal_etl_emr_step(etl_args=etl_args)
    else:
        etl_args = add_portal_etl_emr_step_clinical.add_portal_etl_emr_step(etl_args=etl_args)

    return etl_args
