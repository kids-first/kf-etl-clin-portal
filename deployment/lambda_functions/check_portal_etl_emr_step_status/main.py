
import boto3

def check_portal_etl_emr_step_status(etl_args, context):
    print(f'Check Status of Variant ETL Inputs: {etl_args}')
    cluster_id = etl_args['portalEtlClusterId']
    step_id = etl_args['currentEtlStepId']

    print(f'ClusterID is {cluster_id}')
    current_step_status = get_portal_etl_emr_step_status(cluster_id=cluster_id, step_id=step_id)

    print(f"ETL Cluster Id: {cluster_id} Step Status {current_step_status}")
    emr_cluster_status = get_emr_cluster_status(cluster_id)
    etl_status = calculate_etl_status(current_step_status, emr_cluster_status, etl_args['etlPortalStepsToExecute'], etl_args['currentEtlStep'])

    etl_args['etlStatus'] = etl_status
    etl_args['currentEtlStepStatus'] = current_step_status
    return etl_args

def get_emr_cluster_status(cluster_id : str) -> str :
    client = boto3.client('emr')
    response = client.describe_cluster(
        ClusterId=cluster_id
    )
    return response['Cluster']['Status']['State']

def calculate_etl_status(current_step_status : str, emr_cluster_status : str, etl_variant_steps_to_execute : list, current_step : str) -> str :
    etl_status = "RUNNING"
    if current_step_status == 'COMPLETED':
        if etl_variant_steps_to_execute[-1] == current_step:
            etl_status = "COMPLETED"
    elif current_step_status in ["CANCELLED" ,"FAILED", "INTERRUPTED"]:
        etl_status = "FAILED"

    if emr_cluster_status in ["TERMINATING","TERMINATED","TERMINATED_WITH_ERRORS"]:
        etl_status = "FAILED"

    return etl_status


def get_portal_etl_emr_step_status(cluster_id : str, step_id : str) -> str :
    step_status = "FAILED"
    if cluster_id is not None and step_id is not None:
        client = boto3.client('emr')
        response = client.describe_step(
            ClusterId=cluster_id,
            StepId=step_id
        )
        step_status = response['Step']['Status']['State']
    return step_status


if __name__ == '__main__':
    test_args = {
        'clusterId' : 'j-LX1W8APSM2XB',
        'currentEtlVariantStepId' : 's-05048533OBXXV7C9Z5KR',
    }

    check_portal_etl_emr_step_status(test_args, None)