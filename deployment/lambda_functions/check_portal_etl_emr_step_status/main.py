
import boto3

STEP_FAILED_STATES =  ["CANCELLED" ,"FAILED", "INTERRUPTED"]
EMR_CLUSTER_FAILED_STATES = ["TERMINATING","TERMINATED","TERMINATED_WITH_ERRORS"]

def check_portal_etl_emr_step_status(etl_args, context):
    """
    Checks the status of a specific ETL step and the overall EMR cluster status.

    Args:
        etl_args (dict): ETL configuration arguments.
        context: Context information.

    Returns:
        dict: Updated ETL arguments.
    """

    print(f'Check Status of Variant ETL Inputs: {etl_args}')
    cluster_id = etl_args['portalEtlClusterId']
    step_ids = etl_args['currentEtlStepIds']

    step_statuses = [get_portal_etl_emr_step_status(cluster_id=cluster_id, step_id=step_id) for step_id in step_ids]
    emr_cluster_status = get_emr_cluster_status(cluster_id)
    etl_status = calculate_etl_status(step_statuses, emr_cluster_status, etl_args['input']['etlStepsToExecute'], etl_args['currentEtlSteps'])

    print(f'ClusterID is {cluster_id}')
    print(f"ETL Cluster Id: {cluster_id} Step Status {step_statuses}")

    etl_args['etlStatus'] = etl_status
    etl_args['currentEtlStepStatuses'] = step_statuses
    return etl_args

def get_emr_cluster_status(cluster_id : str) -> str :
    """
    Retrieves the status of an EMR cluster.

    Args:
        cluster_id (str): The ID of the EMR cluster.

    Returns:
        str: The state of the EMR cluster.
    """

    client = boto3.client('emr')
    response = client.describe_cluster(
        ClusterId=cluster_id
    )
    return response['Cluster']['Status']['State']

def calculate_etl_status(current_step_statuses : list, emr_cluster_status : str, etl_variant_steps_to_execute : list, current_steps : list) -> str :
    """
    Calculates the overall ETL status based on step and cluster statuses.

    Args:
        current_step_status (str): The status of the current ETL step.
        emr_cluster_status (str): The status of the EMR cluster.
        etl_variant_steps_to_execute (list): List of ETL steps to execute.
        current_step (str): The current ETL step.

    Returns:
        str: The calculated ETL status.
    """

    steps_completed = [current_step_status for current_step_status in current_step_statuses if current_step_status == 'COMPLETED']
    steps_failed = [current_step_status for current_step_status in current_step_statuses if current_step_status in STEP_FAILED_STATES ]

    if len(steps_completed) == len(current_step_statuses) and current_steps[-1].startswith(etl_variant_steps_to_execute[-1]):
        etl_status = "COMPLETED"
    elif len(steps_completed) == len(current_steps):
        etl_status = "SUBMIT_STEP"
    elif len(steps_failed) > 0 or emr_cluster_status in EMR_CLUSTER_FAILED_STATES:
        etl_status = "FAILED"
    else:
        etl_status = "RUNNING"
    return etl_status


def get_portal_etl_emr_step_status(cluster_id : str, step_id : str) -> str :
    """
    Retrieves the status of a specific ETL step in an EMR cluster.

    Args:
        cluster_id (str): The ID of the EMR cluster.
        step_id (str): The ID of the ETL step.

    Returns:
        str: The status of the ETL step.
    """

    client = boto3.client('emr')
    response = client.describe_step(
        ClusterId=cluster_id,
        StepId=step_id
    )
    return response['Step']['Status']['State']

if __name__ == '__main__':
    test_args = {
        'clusterId' : 'j-LX1W8APSM2XB',
        'currentEtlVariantStepId' : 's-05048533OBXXV7C9Z5KR',
    }

    check_portal_etl_emr_step_status(test_args, None)