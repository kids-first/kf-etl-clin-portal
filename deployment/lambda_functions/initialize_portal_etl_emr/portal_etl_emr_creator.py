import json
import boto3
from abc import ABC, abstractmethod

instance_count_map = {
    'xsmall': 1,
    'small': 5,
    'medium': 10,
    'large': 20,
    'xlarge': 30
}


def generate_emr_name(env: str, release_id: str, studies: list) -> str:
    """
    Generates an EMR cluster name based on environment, release ID, and study IDs.

    Args:
        env (str): Environment.
        release_id (str): Release ID.
        studies (list): List of study IDs.

    Returns:
        str: Generated EMR cluster name.
    """

    emr_name = f'Portal ETL-{env}-{release_id} {" ".join(studies)}'

    # Check if length exceeds 256 char limit
    max_length = 256
    if len(emr_name) > max_length:
        emr_name = emr_name[:max_length - 3] + "..."
    return emr_name


class PortalEtlEmrCreator(ABC):
    def __init__(self, etl_args: dict):
        self.etl_args = etl_args
        self.env = etl_args['environment']
        self.bucket = etl_args['etlPortalBucket']
        self.instance_profile = etl_args['emrInstanceProfile']
        self.service_role = etl_args['emrServiceRole']
        self.subnet = etl_args['emrEc2Subnet']
        # User Input
        self.__etl_user_input = etl_args['input']
        self.release_id = self.__etl_user_input['releaseId']
        self.studies = self.__etl_user_input['studyIds']
        self.run_genomic_etl = self.__etl_user_input.get('runGenomicEtl', False)
        self.cluster_size = self.__etl_user_input.get('clusterSize', 'medium')
        self.custom_emr_name = self.__etl_user_input.get('portalEtlName')

    def create_emr(self) -> str:
        emr_name = self.custom_emr_name if self.custom_emr_name is not None else (
            generate_emr_name(self.env, self.release_id, self.studies))

        spark_config = self.read_spark_config()

        instance_count = instance_count_map[self.cluster_size.lower()]

        instance_config = self.get_instance_config(instance_count)

        bootstrap_actions_config = self.get_bootstrap_actions()

        step_concurrency = self.get_step_concurrency()

        client = boto3.client('emr', region_name='us-east-1')

        response = client.run_job_flow(
            Name=emr_name,
            ReleaseLabel='emr-6.11.0',
            LogUri=f's3n://{self.bucket}/jobs/elasticmapreduce/',
            Instances=instance_config,
            Applications=[
                {'Name': 'Hadoop'},
                {'Name': 'Spark'}
            ],
            ServiceRole=self.service_role,
            JobFlowRole=self.instance_profile,
            BootstrapActions=[
                bootstrap_actions_config
            ],
            Configurations=spark_config,
            StepConcurrencyLevel=step_concurrency,
            AutoTerminationPolicy={
                'IdleTimeout': 600
            }
        )

        return response['JobFlowId']

    @abstractmethod
    def get_step_concurrency(self) -> int:
        pass

    @abstractmethod
    def get_bootstrap_actions(self):
        pass

    @abstractmethod
    def get_instance_config(self, instance_count: int):
        pass

    @abstractmethod
    def get_spark_config_file_path(self) -> str:
        pass

    def read_spark_config(self) -> str:
        """
        Reads the Spark configuration from a JSON file.

        Returns:
            str: Spark configuration as a JSON string.
        """
        with open(self.get_spark_config_file_path(), 'r') as spark_config_file:
            spark_config = json.load(spark_config_file)
        return spark_config
