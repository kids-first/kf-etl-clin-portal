
import boto3

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
    step_id = etl_args['currentEtlStepId']

    current_step_status = get_portal_etl_emr_step_status(cluster_id=cluster_id, step_id=step_id)
    emr_cluster_status = get_emr_cluster_status(cluster_id)
    etl_status = calculate_etl_status(current_step_status, emr_cluster_status, etl_args['input']['etlStepsToExecute'], etl_args['currentEtlStep'])

    print(f'ClusterID is {cluster_id}')
    print(f"ETL Cluster Id: {cluster_id} Step Status {current_step_status}")

    etl_args['etlStatus'] = etl_status
    etl_args['currentEtlStepStatus'] = current_step_status
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

def calculate_etl_status(current_step_status : str, emr_cluster_status : str, etl_variant_steps_to_execute : list, current_step : str) -> str :
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

    if current_step_status == 'COMPLETED' and etl_variant_steps_to_execute[-1] == current_step:
        etl_status = "COMPLETED"
    elif current_step_status in ["CANCELLED" ,"FAILED", "INTERRUPTED"] or emr_cluster_status in ["TERMINATING","TERMINATED","TERMINATED_WITH_ERRORS"]:
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