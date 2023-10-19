import boto3

from portal_etl_emr_creator import PortalEtlEmrCreator


class ClinicalPortalEtlEmrCreator(PortalEtlEmrCreator):
    def __init__(self, etl_args):
        super(ClinicalPortalEtlEmrCreator, self).__init__(etl_args)

    def get_step_concurrency(self) -> int:
        return 1

    def get_bootstrap_actions(self):
        return [
            {
                'Name': 'Install Java 11',
                'ScriptBootstrapAction': {
                    'Path': f's3://{self.bucket}/jobs/bootstrap-actions/install-java11.sh'
                }
            }
        ]

    def get_instance_config(self, instance_count: int):
        instance_type = 'm5.8xlarge'

        # Grab Security Group Ids
        service_security_group = get_security_group_id(group_name=f'ElasticMapReduce-ServiceAccess-{self.env}-*')
        master_security_group = get_security_group_id(group_name=f'ElasticMapReduce-Master-Private-{self.env}-*')
        slave_security_group = get_security_group_id(group_name=f'ElasticMapReduce-Slave-Private-{self.env}-*')

        return {
            'InstanceGroups': get_instance_group_config(instance_type, instance_count),
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': self.subnet,
            'EmrManagedMasterSecurityGroup': master_security_group,
            'EmrManagedSlaveSecurityGroup': slave_security_group,
            'ServiceAccessSecurityGroup': service_security_group,
        }

    def get_spark_config_file_path(self) -> str:
        return 'conf/spark-config-clinical.json'


def get_security_group_id(group_name: str) -> str:
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
                'Name': 'group-name',
                'Values': [group_name]
            },
        ]
    )
    return response['SecurityGroups'][0]['GroupId']


def get_instance_group_config(instance_type: str, instance_count: int):
    return [
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
    ]
