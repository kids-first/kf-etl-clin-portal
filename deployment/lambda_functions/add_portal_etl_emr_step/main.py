import boto3
import sys

default_portal_etl_steps = ['Cleanup jars', 'Download and Run Fhavro-export', 'Normalize Dataservice', 'Normalize Clinical',
                            'Enrich All', 'Prepare Index', 'Index Study', 'Index Participant', 'Index File', 'Index Biospecimen']

def add_portal_etl_emr_step(etl_args, context):
    print(f'Add Step to Variant ETL {etl_args}')

    (etl_portal_steps_to_execute, current_step) = grab_etl_step_and_list_of_steps(etl_args=etl_args)

    print(f'Attempting to get Next Step in ETL currentStep={current_step}, list of steps {etl_portal_steps_to_execute}')
    next_etl_step = get_next_step(etl_portal_steps_to_execute, current_step)

    # Extract Data From Input
    variant_etl_cluster_id = etl_args['portalEtlClusterId']
    elastic_search_endpoint = etl_args['esEnpoint']

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

    etl_args['currentEtlStepId'] = response["StepIds"][0]
    etl_args['currentEtlStep'] = next_etl_step
    return etl_args

def grab_etl_step_and_list_of_steps(etl_args: dict) -> tuple:
    # Grab Current ETL Step and List of Steps to execute
    etl_portal_steps_to_execute = etl_args.get('etlPortalStepsToExecute')
    current_step = etl_args.get('currentEtlPortalStep')

    if etl_portal_steps_to_execute is None:
        etl_portal_steps_to_execute = default_portal_etl_steps
        etl_args['etlPortalStepsToExecute'] = default_portal_etl_steps

    return (etl_portal_steps_to_execute, current_step)

def get_next_step(etl_variant_steps_to_execute : list, current_step : str):
    if current_step is None or len(current_step) < 1:
        return etl_variant_steps_to_execute[0]
    try:
        index = etl_variant_steps_to_execute.index(current_step)
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
        "Type": "CUSTOM_JAR",
        "Name": "Cleanup jars",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "Jar": "command-runner.jar",
        "Args": [
            "bash", "-c",
            "sudo rm -f /usr/lib/spark/jars/spark-avro.jar"
        ]
    }

def generate_download_and_run_fhavro_export_step(etl_config : dict):
    return {
        "Type":"CUSTOM_JAR",
        "Name":"Download and Run Fhavro-export",
        "ActionOnFailure":"TERMINATE_CLUSTER",
        "Jar":"command-runner.jar",
        "Args":[
            "bash","-c",
            f"aws s3 cp s3://{etl_config['etlVariantBucket']}/jobs/fhavro-export.jar /home/hadoop; export FHIR_URL='{etl_config['fhirUrl']}'; export BUCKET='{etl_config['etlVariantBucket']}'; cd /home/hadoop;/usr/lib/jvm/java-11-amazon-corretto.x86_64/bin/java -jar fhavro-export.jar {etl_config['releaseId']} {' '.join(etl_config['studyIds'])} default"
        ]
    }

def generate_portal_etl_step(class_name : str, step_name : str, etl_config : dict):
    return {
        "Args": [
            "spark-submit",
            "--packages",
            "com.typesafe.play:play-ahc-ws-standalone_2.12:2.0.3",
            "--deploy-mode",
            "client",
            "--class",
            f"{class_name}",
            f"s3a://{etl_config['etlVariantBucket']}/jobs/etl.jar",
            "--config", f"config/{etl_config['env']}-{etl_config['project']}.conf",
            "--steps", "default",
            "--release-id", f"{etl_config['releaseId']}",
            "--study-id", f"{' '.join(etl_config['studyIds'])}"
        ],
        "Type": "CUSTOM_JAR",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "Jar": "command-runner.jar",
        "Properties": "",
        "Name": f"{step_name}"
    }

def generate_indexing_step(index_centric : str, step_name : str, etl_config : dict, elastic_search_endpoint : str):
    return {
     "Args": [
       "spark-submit",
       "--deploy-mode",
       "client",
       "--packages",
       "org.elasticsearch:elasticsearch-spark-30_2.12:7.17.12",
       "--class",
       "bio.ferlab.etl.indexed.clinical.RunIndexClinical",
       f"s3a://{etl_config['etlVariantBucket']}/jobs/etl.jar",
       f"{elastic_search_endpoint}",
       "443",
       f"{etl_config['releaseId']}",
       f"{' '.join(etl_config['studyIds'])}",
       f"{index_centric}",
       f"config/{etl_config['env']}-{etl_config['project']}.conf"
     ],
     "Type": "CUSTOM_JAR",
     "ActionOnFailure": "TERMINATE_CLUSTER",
     "Jar": "command-runner.jar",
     "Properties": "",
     "Name": f"{step_name}"

    }

variant_etl_map = {
    'Cleanup jars' : lambda etl_config , elastic_search_endpoint : generate_cleanup_jars_step(),

    'Download and Run Fhavro-export' : lambda etl_config , elastic_search_endpoint:
        generate_download_and_run_fhavro_export_step(etl_config=etl_config),

    'Normalize Dataservice' : lambda etl_config , elastic_search_endpoint : generate_portal_etl_step(
        "bio.ferlab.etl.normalized.dataservice.RunNormalizeDataservice", "Normalize Dataservice", etl_config=etl_config),

    'Normalize Clinical' : lambda etl_config , elastic_search_endpoint : generate_portal_etl_step(
        "bio.ferlab.etl.normalized.clinical.RunNormalizeClinical", "Normalize Clinical", etl_config=etl_config),

    'Enrich All' : lambda etl_config , elastic_search_endpoint : generate_portal_etl_step(
        "bio.ferlab.etl.enriched.clinical.RunEnrichClinical", "Enrich All", etl_config=etl_config),

    'Prepare Index' : lambda etl_config , elastic_search_endpoint : generate_portal_etl_step(
        "bio.ferlab.etl.prepared.clinical.RunPrepareClinical", "Prepare Index", etl_config=etl_config),

    'Index Study' : lambda etl_config , elastic_search_endpoint : generate_indexing_step(
        "study_centric", "Index Study", etl_config=etl_config, elastic_search_endpoint=elastic_search_endpoint),

    'Index Participant' : lambda etl_config , elastic_search_endpoint : generate_indexing_step(
        "participant_centric", "Index Participant", etl_config=etl_config, elastic_search_endpoint=elastic_search_endpoint),

    'Index File' : lambda etl_config , elastic_search_endpoint : generate_indexing_step(
        "file_centric", "Index File'", etl_config=etl_config, elastic_search_endpoint=elastic_search_endpoint),

    'Index Biospecimen' : lambda etl_config , elastic_search_endpoint : generate_indexing_step(
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