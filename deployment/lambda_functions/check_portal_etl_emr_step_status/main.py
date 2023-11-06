import boto3
import botocore.exceptions

STEP_FAILED_STATES = ["CANCELLED", "FAILED", "INTERRUPTED"]
EMR_CLUSTER_FAILED_STATES = ["TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS"]
COMPLETED = "COMPLETED"
FAILED = "FAILED"
RUNNING = "RUNNING"
SUBMIT_STEP = "SUBMIT_STEP"


class PortalETLMonitor:
    def __init__(self, etl_args: dict):
        self.etl_args = etl_args
        self.cluster_id = etl_args['portalEtlClusterId']
        self.step_ids = etl_args['currentEtlStepIds']
        self.step_statuses = []
        self.emr_client = boto3.client('emr')

    def get_etl_status(self):
        self.step_statuses = self.get_portal_etl_emr_step_statuses(self.step_ids)
        emr_cluster_status = self.get_emr_cluster_status()

        etl_steps_to_execute = self.etl_args['input']['etlStepsToExecute']
        current_etl_steps_in_queue = self.etl_args['currentEtlSteps']
        etl_status = calculate_etl_status(self.step_statuses, emr_cluster_status, etl_steps_to_execute,
                                          current_etl_steps_in_queue)
        print(f'ClusterID is {self.cluster_id}')
        print(f"ETL Cluster Id: {self.cluster_id} Step Status {self.step_statuses}")
        return etl_status

    def get_emr_cluster_status(self) -> str:
        """
        Retrieves the status of an EMR cluster.

        Returns:
            str: The state of the EMR cluster.
        """

        try:
            response = self.emr_client.describe_cluster(
                ClusterId=self.cluster_id
            )
            return response['Cluster']['Status']['State']
        except botocore.exceptions.ClientError as e:
            print(f"Error Getting EMR Cluster Status: {e}")
            return "FAILED"

    def get_portal_etl_step_statuses_raw(self, step_ids: list):
        try:
            return self.emr_client.list_steps(
                ClusterId=self.cluster_id,
                StepIds=step_ids,
            )

        except botocore.exceptions.ClientError as e:
            print(f"Error Getting EMR Cluster Status: {e}")
            return ["FAILED"]

    def get_portal_etl_step_statues_in_chunks(self, step_ids, chunk_size=10):
        results = []
        for i in range(0, len(step_ids), chunk_size):
            ids_to_query = step_ids[i:i + chunk_size]
            result = self.get_portal_etl_step_statuses_raw(ids_to_query)['Steps']
            results.extend(result)
        return results

    def get_portal_etl_emr_step_statuses(self, step_ids: list) -> list:

        response = self.get_portal_etl_step_statues_in_chunks(step_ids, 10)

        # Check if any steps failed
        failed_steps = [step_status for step_status in response if step_status['Status']['State'] == 'FAILED']
        if failed_steps:
            for failed_step in failed_steps:
                print(f" Step ID: {failed_step['Id']} Failed")
                print(failed_step['FailureDetails'])

        return [step_status['Status']['State'] for step_status in response]

    def get_portal_etl_emr_step_status(self, step_id: str) -> str:
        """
        Retrieves the status of a specific ETL step in an EMR cluster.

        Args:
            step_id (str): The ID of the ETL step.

        Returns:
            str: The status of the ETL step.
        """

        try:
            response = self.emr_client.describe_step(
                ClusterId=self.cluster_id,
                StepId=step_id
            )
            return response['Step']['Status']['State']
        except botocore.exceptions.ClientError as e:
            print(f"Error Getting EMR Cluster Status: {e}")
            return "FAILED"

    def get_step_statuses(self) -> list:
        return self.step_statuses


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

    portal_etl_monitor = PortalETLMonitor(etl_args=etl_args)
    etl_args['etlStatus'] = portal_etl_monitor.get_etl_status()
    etl_args['currentEtlStepStatuses'] = portal_etl_monitor.step_statuses
    return etl_args


def calculate_etl_status(current_step_statuses: list, emr_cluster_status: str, etl_variant_steps_to_execute: list,
                         current_steps: list) -> str:
    """
    Calculates the overall ETL status based on step and cluster statuses.

    Args:
        current_step_status (str): The status of the current ETL step.
        emr_cluster_status (str): The status of the EMR cluster.
        etl_variant_steps_to_execute (list): List of ETL steps to execute.
        current_step (str): The current ETL step.

    Returns:
        str: The calculated ETL status.
        :param etl_variant_steps_to_execute:
        :param emr_cluster_status:
        :param current_steps:
        :param current_step_statuses:
    """

    if not current_step_statuses or not current_steps:
        return "INVALID_INPUT"

    if (all(status == COMPLETED for status in current_step_statuses) and
            current_steps[-1].startswith(etl_variant_steps_to_execute[-1])):
        return COMPLETED
    elif all(status == COMPLETED for status in current_step_statuses):
        return SUBMIT_STEP
    elif (any(status in STEP_FAILED_STATES for status in current_step_statuses) or
          emr_cluster_status in EMR_CLUSTER_FAILED_STATES):
        return FAILED
    else:
        return RUNNING


if __name__ == '__main__':
    test_args = {
        'clusterId': 'j-LX1W8APSM2XB',
        'currentEtlVariantStepId': 's-05048533OBXXV7C9Z5KR',
    }
