
import boto3
import json

cluster_size_map = {
    'small' : 'm5.xlarge',
    'large' : 'm5.xlarge'
}

def initialize_portal_etl_emr(etl_args, context):
    print('Initiate Portal ETL EMR')
    print(f'Inputs: ${etl_args}')

    # Extract Data From Inpu
    env = etl_args['environment']
    bucket = etl_args['etlPortalBucket']
    instance_profile = etl_args['emrInstanceProfile']
    service_role = etl_args['emrServiceRole']
    subnet = etl_args['emrEc2Subnet']

    # Portal Input
    etl_user_input = etl_args['input']
    custom_emr_name = etl_user_input['portalEtlName']
    release_id = etl_user_input['releaseId']
    studies = etl_user_input['studyIds']
    instance_count = etl_user_input['instanceCount']
    cluster_size = etl_user_input['clusterSize']

    emr_name = custom_emr_name if custom_emr_name is not None else generate_emr_name(env, release_id, studies)

    # Grab Security Group Ids
    service_security_group_id = get_security_group_id(group_name=f'ElasticMapReduce-ServiceAccess-{env}-*')
    master_security_group_id = get_security_group_id(group_name=f'ElasticMapReduce-Master-Private-{env}-*')
    slave_security_group_id = get_security_group_id(group_name=f'ElasticMapReduce-Slave-Private-{env}-*')


    spark_config = read_spark_config()

    emr_id = create_emr_cluster(emr_name=emr_name, bucket=bucket, instance_count=instance_count, instance_profile=instance_profile,
                       service_role=service_role, subnet=subnet, spark_config=spark_config,
                        service_security_group=service_security_group_id, master_security_group=master_security_group_id,
                         slave_security_group=slave_security_group_id, cluster_size=cluster_size)

    etl_args['portalEtlClusterId'] = emr_id
    return etl_args


def get_security_group_id(group_name : str) -> str:
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
    emr_name = f'Portal ETL-{env}-{release_id} {" ".join(studies_list)}'

    # Check if length exceeds 256 char limit
    max_length = 256
    if len(emr_name) > max_length:
        emr_name = emr_name[:max_length - 3] + "..."
    return emr_name

def read_spark_config() -> str:
    spark_config = ''
    with open('conf/spark-config-clinical.json', 'r') as spark_config_file:
        spark_config = json.load(spark_config_file)
    return spark_config

def create_emr_cluster( emr_name : str, bucket : str, instance_count : int, instance_profile : str,
                        service_role : str, subnet : str, spark_config : str,
                        service_security_group : str, master_security_group : str,
                        slave_security_group : str,  cluster_size='small') -> str:
    instance_type = cluster_size_map[cluster_size]
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