import boto3
import sys

# Default list of Portal ETL Steps
DEFAULT_PORTAL_ETL_STEPS = ['cleanup jars', 'download and run fhavro-export', 'normalize dataservice', 'normalize clinical',
                            'enrich all', 'prepare index', 'index study', 'index participant', 'index file', 'index biospecimen']

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

    (etl_portal_steps_to_execute, current_step) = grab_etl_step_and_list_of_steps(etl_args=etl_args)

    print(f'Attempting to get Next Step in ETL currentStep={current_step}, list of steps {etl_portal_steps_to_execute}')
    next_etl_step = get_next_step(etl_portal_steps_to_execute, current_step)

    # Extract Data From Input
    variant_etl_cluster_id = etl_args['portalEtlClusterId']
    elastic_search_endpoint = etl_args['esEndpoint']

    if next_etl_step is None:
        print('Next Step Could not be defined.... Exiting ETL')
        sys.exit()

    print(f'Next Step to submit to ETL: ID: {variant_etl_cluster_id}, Next Step: {next_etl_step}')
    client = boto3.client('emr', region_name='us-east-1')
    response = client.add_job_flow_steps(
        JobFlowId=variant_etl_cluster_id,
        Steps=[variant_etl_map[next_etl_step](etl_config=etl_args, elastic_search_endpoint=elastic_search_endpoint)]
    )

    print(f'Submitted Next Step: StepId={response["StepIds"][0]}')

    etl_args['currentEtlStepIds'] = [response["StepIds"][0]]
    etl_args['currentEtlSteps'] = [next_etl_step]
    return etl_args

def grab_etl_step_and_list_of_steps(etl_args: dict) -> tuple:
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
    current_step = etl_args.get('currentEtlSteps')

    if etl_portal_steps_to_execute is None:
        etl_portal_steps_to_execute = DEFAULT_PORTAL_ETL_STEPS
        # Hack to remove normalize dataservice step when in INCLUDE account
        if etl_args['account'] == 'include':
            etl_portal_steps_to_execute.remove('normalize dataservice')
        etl_args['input']['etlStepsToExecute'] = etl_portal_steps_to_execute
    # Validate User's Custom ETL Portal Steps before submitting first step (currentEtlStep is None)
    elif current_step is None:
        validate_custom_etl_portal_steps_to_execute(etl_portal_steps_to_execute)

    return (etl_portal_steps_to_execute, current_step)

def validate_custom_etl_portal_steps_to_execute(etl_portal_steps_to_execute : list):
    """
    Validates user's custom ETL portal steps to execute.

    Args:
        etl_portal_steps_to_execute (list): List of ETL steps to execute.

    Returns:
        bool: True if custom ETL steps are valid, False otherwise.
    """
    custom_etl_steps_valid = all(etl_step.lower() in DEFAULT_PORTAL_ETL_STEPS for etl_step in etl_portal_steps_to_execute)
    if not custom_etl_steps_valid or len(etl_portal_steps_to_execute) > len(DEFAULT_PORTAL_ETL_STEPS):
        print(f'Custom Portal ETL Steps not valid: steps input: ${etl_portal_steps_to_execute}')
        sys.exit('Invalid Custom ETL Steps')
    return

def get_next_step(etl_variant_steps_to_execute : list, current_step : list):
    """
    Gets the next ETL step in the process.

    Args:
        etl_variant_steps_to_execute (list): List of ETL steps to execute.
        current_step (str): Current ETL step.

    Returns:
        str: The next ETL step.
    """
    if current_step is None or len(current_step) < 0:
        return etl_variant_steps_to_execute[0]
    try:
        index = etl_variant_steps_to_execute.index(current_step[-1])
        if index < len(etl_variant_steps_to_execute) - 1:
            return etl_variant_steps_to_execute[index + 1]
        else:
            return None  # No next step
    except ValueError:
        return None  # Current step not found in the list

###
# Helper Functions to generate Variant ETL Steps
###
def generate_cleanup_jars_step():
    return {
        "HadoopJarStep" : {
            "Args": [
                "bash", "-c",
                "sudo rm -f /usr/lib/spark/jars/spark-avro.jar"
            ],
            "Jar": "command-runner.jar"
        },
        "Name": "Cleanup jars",
        "ActionOnFailure": "CONTINUE"
    }

def generate_download_and_run_fhavro_export_step(etl_config : dict):
    etl_portal_bucket = etl_config['etlPortalBucket']
    fhir_url = etl_config['input']['fhirUrl']
    release_id = etl_config['input']['releaseId']
    study_ids = ','.join(etl_config['input']['studyIds'])

    fhavro_export_args = [
        "aws", "s3", "cp",
        f"s3://{etl_portal_bucket}/jobs/fhavro-export.jar", "/home/hadoop;",
        f"export FHIR_URL='{fhir_url}'; export BUCKET='{etl_portal_bucket}';",
        "cd /home/hadoop;","/usr/lib/jvm/java-11-amazon-corretto.x86_64/bin/java -jar",
        f"fhavro-export.jar {release_id} {study_ids} default y"
    ]

    return {
        "HadoopJarStep": {
            "Args": ["bash", "-c", " ".join(fhavro_export_args)],
            "Jar": "command-runner.jar"
        },
        "Name": "Download and Run Fhavro-export",
        "ActionOnFailure": "CONTINUE"
    }

def generate_normalize_portal_etl_step(class_name : str, step_name : str, etl_config : dict):
    etl_portal_bucket = etl_config['etlPortalBucket']
    env = etl_config['environment']
    account = etl_config['account']
    release_id = etl_config['input']['releaseId']
    study_ids = ','.join(etl_config['input']['studyIds'])
    return {
        "HadoopJarStep": {
            "Args": [
                "spark-submit",
                "--packages",
                "com.typesafe.play:play-ahc-ws-standalone_2.12:2.0.3",
                "--deploy-mode",
                "client",
                "--class",
                f"{class_name}",
                f"s3a://{etl_portal_bucket}/jobs/etl.jar",
                "--config", f"config/{env}-{account}.conf",
                "--steps", "default",
                "--release-id", f"{release_id}",
                "--study-id", f"{study_ids}"
            ],
            "Jar": "command-runner.jar"
        },
        "Name": f"{step_name}",
        "ActionOnFailure": "CONTINUE"
    }

def generate_portal_etl_step(class_name : str, step_name : str, etl_config : dict):
    etl_portal_bucket = etl_config['etlPortalBucket']
    env = etl_config['environment']
    account = etl_config['account']
    study_ids = ','.join(etl_config['input']['studyIds'])
    return {
        "HadoopJarStep": {
            "Args": [
                "spark-submit",
                "--packages",
                "com.typesafe.play:play-ahc-ws-standalone_2.12:2.0.3",
                "--deploy-mode",
                "client",
                "--class",
                f"{class_name}",
                f"s3a://{etl_portal_bucket}/jobs/etl.jar",
                f"all",
                "--config", f"config/{env}-{account}.conf",
                "--steps", "default",
                "--study-id", f"{study_ids}"
            ],
            "Jar": "command-runner.jar"
        },
        "Name": f"{step_name}",
        "ActionOnFailure": "CONTINUE"
    }

def generate_indexing_step(index_centric : str, step_name : str, etl_config : dict, elastic_search_endpoint : str):
    etl_portal_bucket = etl_config['etlPortalBucket']
    env = etl_config['environment']
    account = etl_config['account']
    release_id = etl_config['input']['releaseId']
    study_ids = ','.join(etl_config['input']['studyIds'])
    return {
        "HadoopJarStep": {
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--packages",
                "org.elasticsearch:elasticsearch-spark-30_2.12:7.17.12",
                "--class",
                "bio.ferlab.etl.indexed.clinical.RunIndexClinical",
                f"s3a://{etl_portal_bucket}/jobs/etl.jar",
                f"{elastic_search_endpoint}",
                "443",
                f"{release_id}",
                f"{study_ids}",
                f"{index_centric}",
                f"config/{env}-{account}.conf"
            ],
            "Jar": "command-runner.jar"
        },
        "Name": f"{step_name}",
        "ActionOnFailure": "CONTINUE"
    }

variant_etl_map = {
    'cleanup jars' : lambda etl_config , elastic_search_endpoint : generate_cleanup_jars_step(),

    'download and run fhavro-export' : lambda etl_config , elastic_search_endpoint:
        generate_download_and_run_fhavro_export_step(etl_config=etl_config),

    'normalize dataservice' : lambda etl_config , elastic_search_endpoint : generate_normalize_portal_etl_step(
        "bio.ferlab.etl.normalized.dataservice.RunNormalizeDataservice", "Normalize Dataservice", etl_config=etl_config),

    'normalize clinical' : lambda etl_config , elastic_search_endpoint : generate_normalize_portal_etl_step(
        "bio.ferlab.etl.normalized.clinical.RunNormalizeClinical", "Normalize Clinical", etl_config=etl_config),

    'enrich all' : lambda etl_config , elastic_search_endpoint : generate_portal_etl_step(
        "bio.ferlab.etl.enriched.clinical.RunEnrichClinical", "Enrich All", etl_config=etl_config),

    'prepare index' : lambda etl_config , elastic_search_endpoint : generate_portal_etl_step(
        "bio.ferlab.etl.prepared.clinical.RunPrepareClinical", "Prepare Index", etl_config=etl_config),

    'index study' : lambda etl_config , elastic_search_endpoint : generate_indexing_step(
        "study_centric", "Index Study", etl_config=etl_config, elastic_search_endpoint=elastic_search_endpoint),

    'index participant' : lambda etl_config , elastic_search_endpoint : generate_indexing_step(
        "participant_centric", "Index Participant", etl_config=etl_config, elastic_search_endpoint=elastic_search_endpoint),

    'index file' : lambda etl_config , elastic_search_endpoint : generate_indexing_step(
        "file_centric", "Index File'", etl_config=etl_config, elastic_search_endpoint=elastic_search_endpoint),

    'index biospecimen' : lambda etl_config , elastic_search_endpoint : generate_indexing_step(
        "biospecimen_centric", "Index Biospecimen", etl_config=etl_config, elastic_search_endpoint=elastic_search_endpoint),
}

if __name__ == '__main__':
    test1 = 'Cleanup jars'
    test2 = 'Download and Run Fhavro-export'
    test3 = 'Prepare Index'
    test4 = 'Index Biospecimen'

    test_etl_config = {
        'env': 'qa',
        'releaseId': 're_004',
        'studyIds': ['SD_65064P2Z', 'StudyB'],
        'etlVariantBucket': 'kf-strides-232196027141-datalake-qa',
        'instanceCount': 1,
        'instanceProfile': 'my-instance-profile',
        'clusterSize': 'large',
        'instanceProfile': 'kf-variant-emr-ec2-qa-profile',
        'serviceRole' : 'kf-variant-emr-qa-role',
        'project': 'kf-strides',
        'fhirUrl': 'http://test'
    }
    elastic_search_endpoint = 'test'

    print(variant_etl_map.get(test1)(test_etl_config, elastic_search_endpoint))
    print(variant_etl_map.get(test2)(test_etl_config, elastic_search_endpoint))
    print(variant_etl_map.get(test3)(test_etl_config, elastic_search_endpoint))
    print(variant_etl_map.get(test4)(test_etl_config, elastic_search_endpoint))