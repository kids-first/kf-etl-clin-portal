import boto3
import sys

# Default list of Portal ETL Steps
DEFAULT_PORTAL_ETL_STEPS = ['normalize-snv', 'normalize-consequences', 'enrich-variant', 'enrich-consequences',
                            'prepare-variant-centric',
                            'prepare-variant-suggestion', 'prepare-gene-centric', 'prepare-gene-suggestion']


def add_portal_etl_emr_step(etl_args):
    """
    Adds a new ETL step to the EMR cluster for portal ETL.

    Args:
        etl_args (dict): ETL configuration arguments.
        context: Context information.

    Returns:
        dict: Updated ETL arguments.
    """
    print(f'Add Step to Variant ETL {etl_args}')

    etl_portal_steps_to_execute = grab_etl_steps_to_execute(etl_args=etl_args)
    current_etl_steps = etl_args.get('currentEtlSteps')
    study_ids = etl_args['input'].get('studyIds')

    print(
        f'Attempting to get Next Step in ETL currentStep={current_etl_steps}, list of steps {etl_portal_steps_to_execute}')
    next_etl_steps = get_next_step(etl_portal_steps_to_execute, current_etl_steps, study_ids)

    # Extract Data From Input
    variant_etl_cluster_id = etl_args['portalEtlClusterId']

    if next_etl_steps is None or len(next_etl_steps) < 1:
        print('Next Step Could not be defined.... Exiting ETL')
        sys.exit()

    print(f'Next Step to submit to ETL: ID: {variant_etl_cluster_id}, Next Steps: {next_etl_steps}')
    client = boto3.client('emr', region_name='us-east-1')
    response = client.add_job_flow_steps(
        JobFlowId=variant_etl_cluster_id,
        Steps=[variant_etl_map[next_etl_step[0]](etl_config=etl_args, study_id=next_etl_step[1]) for next_etl_step in
               next_etl_steps]
    )

    print(f'Submitted Next Steps: StepIds={response["StepIds"]}')

    etl_args['currentEtlStepIds'] = response["StepIds"]
    etl_args['currentEtlSteps'] = [etl_step[0] if etl_step[1] is None else f'{etl_step[0]}-{etl_step[1]}' for etl_step
                                   in next_etl_steps]
    return etl_args


def grab_etl_steps_to_execute(etl_args: dict) -> list:
    """
    Retrieves the current ETL step and list of steps to execute.

    Args:
        etl_args (dict): ETL configuration arguments.

    Returns:
        tuple: A tuple containing the list of steps and the current step.
    """
    # Grab Current ETL Step and List of Steps to execute
    user_input = etl_args['input']
    etl_portal_steps_to_execute = user_input.get('etlStepsToExecute')
    current_step = etl_args.get('currentEtlStep')

    if etl_portal_steps_to_execute is None:
        etl_portal_steps_to_execute = DEFAULT_PORTAL_ETL_STEPS
        etl_args['input']['etlStepsToExecute'] = etl_portal_steps_to_execute
    # Validate User's Custom ETL Portal Steps before submitting first step (currentEtlStep is None)
    elif current_step is None:
        validate_custom_etl_portal_steps_to_execute(etl_portal_steps_to_execute)

    return (etl_portal_steps_to_execute)


def validate_custom_etl_portal_steps_to_execute(etl_portal_steps_to_execute: list):
    """
    Validates user's custom ETL portal steps to execute.

    Args:
        etl_portal_steps_to_execute (list): List of ETL steps to execute.

    Returns:
        bool: True if custom ETL steps are valid, False otherwise.
    """
    custom_etl_steps_valid = all(
        etl_step.lower() in DEFAULT_PORTAL_ETL_STEPS for etl_step in etl_portal_steps_to_execute)
    if not custom_etl_steps_valid or len(etl_portal_steps_to_execute) > len(DEFAULT_PORTAL_ETL_STEPS):
        print(f'Custom Portal ETL Steps not valid: steps input: ${etl_portal_steps_to_execute}')
        sys.exit('Invalid Custom ETL Steps')
    return


def get_next_step(etl_variant_steps_to_execute: list, current_steps: list, study_ids: list) -> list:
    """
    Gets the next ETL step in the process.

    Args:
        etl_variant_steps_to_execute (list): List of ETL steps to execute.
        current_step (str): Current ETL step.

    Returns:
        str: The next ETL step.
    """
    etl_step_prefix = ""
    next_steps_to_execute = []
    if current_steps is None or len(current_steps) < 1:
        etl_step_prefix = etl_variant_steps_to_execute[0]
    else:
        try:
            index = next(
                (i for i, prefix in enumerate(etl_variant_steps_to_execute) if current_steps[-1].startswith(prefix)),
                None)

            if index is not None and index < len(etl_variant_steps_to_execute) - 1:
                etl_step_prefix = etl_variant_steps_to_execute[index + 1]
            else:
                return None  # No next step
        except ValueError:
            return None  # Current step not found in the list

    if etl_step_prefix in ['normalize-snv', 'normalize-consequences']:
        next_steps_to_execute = [(etl_step_prefix, study_id) for study_id in study_ids]
    elif etl_step_prefix.startswith('enrich'):
        next_steps_to_execute = [(etl_step, None) for etl_step in etl_variant_steps_to_execute if
                                 etl_step.startswith('enrich')]
    elif etl_step_prefix.startswith('prepare'):
        next_steps_to_execute = [(etl_step, None) for etl_step in etl_variant_steps_to_execute if
                                 etl_step.startswith('prepare')]
    return next_steps_to_execute


###
# Helper Functions to generate Variant ETL Steps
###
def generate_normalize_snv_portal_etl_step(study_id: str, job: str, etl_config: dict):
    etl_portal_bucket = etl_config['etlPortalBucket']
    env = etl_config['environment']
    account = etl_config['account']
    release_id = etl_config['input']['releaseId']

    return {
        "HadoopJarStep": {
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--class",
                "bio.ferlab.etl.normalized.genomic.RunNormalizeGenomic",
                f"s3a://{etl_portal_bucket}/jobs/etl.jar",
                f"{job}",
                "--config", f"config/{env}-{account}.conf",
                "--steps", "default",
                "--study-id", f"{study_id}",
                "--release-id", f"{release_id}",
                "--vcf-pattern", ".CGP.filtered.deNovo.vep.vcf.gz",
                "--reference-genome-path", "/mnt/GRCh38_full_analysis_set_plus_decoy_hla.fa"
            ],
            "Jar": "command-runner.jar"
        },
        "Name": f"Normalize-{job}-{study_id}",
        "ActionOnFailure": "CONTINUE"
    }


def generate_normalize_consequences_portal_etl_step(study_id: str, job: str, etl_config: dict):
    etl_portal_bucket = etl_config['etlPortalBucket']
    env = etl_config['environment']
    account = etl_config['account']

    return {
        "HadoopJarStep": {
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--class",
                "bio.ferlab.etl.normalized.genomic.RunNormalizeGenomic",
                f"s3a://{etl_portal_bucket}/jobs/etl.jar",
                f"{job}",
                "--config", f"config/{env}-{account}.conf",
                "--steps", "default",
                "--study-id", f"{study_id}",
                "--vcf-pattern", ".CGP.filtered.deNovo.vep.vcf.gz",
                "--reference-genome-path", "/mnt/GRCh38_full_analysis_set_plus_decoy_hla.fa"
            ],
            "Jar": "command-runner.jar"
        },
        "Name": f"Normalize-{job}-{study_id}",
        "ActionOnFailure": "CONTINUE"
    }


def generate_enrich_portal_etl_step(job: str, etl_config: dict):
    etl_portal_bucket = etl_config['etlPortalBucket']
    env = etl_config['environment']
    account = etl_config['account']
    return {
        "HadoopJarStep": {
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--class",
                "bio.ferlab.etl.enriched.genomic.RunEnrichGenomic",
                f"s3a://{etl_portal_bucket}/jobs/etl.jar",
                f"{job}",
                "--config", f"config/{env}-{account}.conf",
                "--steps", "default"
            ],
            "Jar": "command-runner.jar"
        },
        "Name": f"Enrich-{job}",
        "ActionOnFailure": "CONTINUE"
    }


def generate_prepare_portal_etl_step(job: str, etl_config: dict):
    etl_portal_bucket = etl_config['etlPortalBucket']
    env = etl_config['environment']
    account = etl_config['account']
    return {
        "HadoopJarStep": {
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--class",
                "bio.ferlab.etl.prepared.genomic.RunPrepareGenomic",
                f"s3a://{etl_portal_bucket}/jobs/etl.jar",
                "--config", f"config/{env}-{account}.conf",
                "default",
                f"{job}"
            ],
            "Jar": "command-runner.jar"
        },
        "Name": f"Prepare-{job}",
        "ActionOnFailure": "CONTINUE"
    }


variant_etl_map = {
    'normalize-snv': lambda etl_config, study_id: generate_normalize_snv_portal_etl_step(
        study_id=study_id, job="snv", etl_config=etl_config),

    'normalize-consequences': lambda etl_config, study_id: generate_normalize_consequences_portal_etl_step(
        study_id=study_id, job="consequences", etl_config=etl_config),

    'enrich-variant': lambda etl_config, study_id: generate_enrich_portal_etl_step("snv", etl_config=etl_config),

    'enrich-consequences': lambda etl_config, study_id: generate_enrich_portal_etl_step("consequences",
                                                                                        etl_config=etl_config),

    'prepare-variant-centric': lambda etl_config, study_id: generate_prepare_portal_etl_step("variant-centric",
                                                                                             etl_config=etl_config),

    'prepare-variant-suggestion': lambda etl_config, study_id: generate_prepare_portal_etl_step("variant-suggestion",
                                                                                                etl_config=etl_config),

    'prepare-gene-centric': lambda etl_config, study_id: generate_prepare_portal_etl_step("gene-centric",
                                                                                          etl_config=etl_config),

    'prepare-gene-suggestion': lambda etl_config, study_id: generate_prepare_portal_etl_step("gene-suggestion",
                                                                                             etl_config=etl_config),
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
            'studyIds': ['SDTest1', 'SDTest2']
        },
        'portalEtlClusterId': '1234',
    }

    etl_args = add_portal_etl_emr_step(test_etl_config, None)
    print("*****************1*********8")
    print(etl_args)
    etl_args = add_portal_etl_emr_step(test_etl_config, None)
    print("*****************2*********8")
    print(etl_args)
    etl_args = add_portal_etl_emr_step(test_etl_config, None)
    print("*****************3*********8")
    print(etl_args)
    etl_args = add_portal_etl_emr_step(test_etl_config, None)
    print("*****************4*********8")
    print(etl_args)
