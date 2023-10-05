from portal_etl_emr_creator import PortalEtlEmrCreator


class GenomicIndexPortalEtlEmrCreator(PortalEtlEmrCreator):
    def __init__(self, etl_args):
        super(GenomicIndexPortalEtlEmrCreator, self).__init__(etl_args)

    def get_step_concurrency(self) -> int:
        return 1

    def get_bootstrap_actions(self):
        return {
            'Name': 'Install Java 11',
            'ScriptBootstrapAction': {
                'Path': f's3://{self.bucket}/jobs/bootstrap-actions/install-java11.sh'
            }
        }

    def get_instance_config(self, instance_count: int):
        instance_type = 'm5.xlarge'
        ec2_key_name = "flintrock_include" if self.etl_args['account'] == 'include' else "flintrock"
        return {
            'InstanceGroups': get_instance_group_config(instance_type),
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': self.subnet,
            'Ec2KeyName': ec2_key_name
        }

    def get_spark_config_file_path(self) -> str:
        return 'conf/spark-config-clinical.json'


def get_instance_group_config(instance_type: str):
    return [
        {
            'Name': 'Core - 2',
            'Market': 'ON_DEMAND',
            'InstanceRole': 'CORE',
            'InstanceType': 'm5.xlarge',
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
        },
        {
            'Name': 'Master - 1',
            'Market': 'ON_DEMAND',
            'InstanceRole': 'MASTER',
            'InstanceType': instance_type,
            'InstanceCount': 1,
        }
    ]
