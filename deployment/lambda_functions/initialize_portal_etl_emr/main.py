
import boto3
import json


instance_count_map = {
    'xsmall' : 1,
    'small' : 5,
    'medium': 10,
    'large' : 20,
    'xlarge' : 30
}

def initialize_portal_etl_emr(etl_args, context):
    """
    Initializes a Portal ETL EMR cluster based on input arguments.

    Args:
        etl_args (dict): ETL configuration arguments.
        context: Context information.

    Returns:
        dict: Updated ETL arguments.
    """

    print('Initiate Portal ETL EMR')
    print(f'Inputs: ${etl_args}')

    # Force KeyError if data isn't available
    # Extract Data From Input
    env = etl_args['environment']
    bucket = etl_args['etlPortalBucket']
    instance_profile = etl_args['emrInstanceProfile']
    service_role = etl_args['emrServiceRole']
    subnet = etl_args['emrEc2Subnet']
    # Portal Input
    etl_user_input = etl_args['input']
    release_id = etl_user_input['releaseId']
    studies = etl_user_input['studyIds']

    cluster_size = etl_user_input.get('clusterSize', 'medium')
    custom_emr_name = etl_user_input.get('portalEtlName')

    emr_name = custom_emr_name if custom_emr_name is not None else generate_emr_name(env, release_id, studies)

    # Grab Security Group Ids
    service_security_group_id = get_security_group_id(group_name=f'ElasticMapReduce-ServiceAccess-{env}-*')
    master_security_group_id = get_security_group_id(group_name=f'ElasticMapReduce-Master-Private-{env}-*')
    slave_security_group_id = get_security_group_id(group_name=f'ElasticMapReduce-Slave-Private-{env}-*')

    spark_config = read_spark_config()

    emr_id = create_emr_cluster(emr_name=emr_name, bucket=bucket, instance_profile=instance_profile,
                       service_role=service_role, subnet=subnet, spark_config=spark_config,
                        service_security_group=service_security_group_id, master_security_group=master_security_group_id,
                         slave_security_group=slave_security_group_id, cluster_size=cluster_size)

    etl_args['portalEtlClusterId'] = emr_id
    return etl_args

def get_security_group_id(group_name : str) -> str:
    """
    Retrieves the security group ID based on the group name.

    Args:
        group_name (str): Name of the security group.

    Returns:
        str: Security group ID.
    """

    client = boto3.client('ec2')
    response = client.describe_security_groups(
        Filters=[
            {
                'Name' : 'group-name',
                'Values' : [group_name]
            },
        ]
    )
    return response['SecurityGroups'][0]['GroupId']

def generate_emr_name(env : str, release_id : str, studies_list : list) -> str :
    """
    Generates an EMR cluster name based on environment, release ID, and study IDs.

    Args:
        env (str): Environment.
        release_id (str): Release ID.
        studies_list (list): List of study IDs.

    Returns:
        str: Generated EMR cluster name.
    """

    emr_name = f'Portal ETL-{env}-{release_id} {" ".join(studies_list)}'

    # Check if length exceeds 256 char limit
    max_length = 256
    if len(emr_name) > max_length:
        emr_name = emr_name[:max_length - 3] + "..."
    return emr_name

def read_spark_config() -> str:
    """
    Reads the Spark configuration from a JSON file.

    Returns:
        str: Spark configuration as a JSON string.
    """

    spark_config = ''
    with open('conf/spark-config-clinical.json', 'r') as spark_config_file:
        spark_config = json.load(spark_config_file)
    return spark_config

def create_emr_cluster( emr_name : str, bucket : str, instance_profile : str,
                        service_role : str, subnet : str, spark_config : str,
                        service_security_group : str, master_security_group : str,
                        slave_security_group : str,  cluster_size='small') -> str:
    """
    Creates an EMR cluster.

    Args:
        emr_name (str): Name of the EMR cluster.
        bucket (str): S3 bucket for logs.
        instance_profile (str): EMR instance profile.
        service_role (str): EMR service role.
        subnet (str): EC2 subnet ID.
        spark_config (str): Spark configuration as JSON.
        service_security_group (str): Security group for service access.
        master_security_group (str): Security group for master node.
        slave_security_group (str): Security group for slave nodes.
        cluster_size (str, optional): Size of the EMR cluster. Defaults to 'small'.

    Returns:
        str: JobFlowId of the created EMR cluster.
    """

    instance_type = 'm5.8xlarge'
    instance_count = instance_count_map[cluster_size.lower()]
    client = boto3.client('emr', region_name='us-east-1')
    response = client.run_job_flow(
        Name=emr_name,
        ReleaseLabel='emr-6.11.0',
        LogUri=f's3n://{bucket}/jobs/elasticmapreduce/',
        Instances={
            'InstanceGroups' : [
                {
                    'Name': 'Core - 2',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': instance_count,
                },
                {
                    'Name': 'Master - 1',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': instance_type,
                    'InstanceCount': 1,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'SizeInGB': 32,
                                    'VolumeType': 'gp2'
                                },
                                'VolumesPerInstance': 2
                            }
                        ]
                    }
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': subnet,
            'EmrManagedMasterSecurityGroup': master_security_group,
            'EmrManagedSlaveSecurityGroup': slave_security_group,
            'ServiceAccessSecurityGroup': service_security_group,
        },
        Applications=[
            {'Name' : 'Hadoop'},
            {'Name' : 'Spark'}
        ],
        ServiceRole=service_role,
        JobFlowRole=instance_profile,
        BootstrapActions=[
            {
                'Name': 'Install Java 11',
                'ScriptBootstrapAction': {
                    'Path': f's3://{bucket}/jobs/bootstrap-actions/install-java11.sh'
                }
            }
        ],
        Configurations=spark_config,
    )

    return response['JobFlowId']


if __name__ == '__main__':
    test_event = {
    'env': 'qa',
    'releaseId': 're_004',
    'studyIds': ['SD_65064P2Z', 'StudyB'],
    'variantBucket': 'kf-strides-232196027141-datalake-qa',
    'instanceCount': 1,
    'instanceProfile': 'my-instance-profile',
    'clusterSize': 'large',
    'instanceProfile': 'kf-variant-emr-ec2-qa-profile',
    'serviceRole' : 'kf-variant-emr-qa-role',
    'project': 'kf-strides',
    }

    initialize_portal_etl_emr(test_event, None)