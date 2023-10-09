from portal_etl_emr_creator import PortalEtlEmrCreator


class GenomicPortalEtlEmrCreator(PortalEtlEmrCreator):
    def __init__(self, etl_args):
        super(GenomicPortalEtlEmrCreator, self).__init__(etl_args)

    def get_step_concurrency(self) -> int:
        return 2

    def get_bootstrap_actions(self):
        return [
            {
                'Name': 'Install Human Reference Genome',
                'ScriptBootstrapAction': {
                    'Path': f's3://{self.bucket}/jobs/bootstrap-actions/download_human_reference_genome.sh'
                }
            },
            {
                'Name': 'Install Ivy Settings',
                'ScriptBootstrapAction': {
                    'Path': f's3://{self.bucket}/jobs/bootstrap-actions/download_ivy_settings.sh'
                }
            }

        ]

    def get_instance_config(self, instance_count: int):
        instance_type = 'r5.4xlarge'
        ec2_key_name = "flintrock_include" if self.etl_args['account'] == 'include' else "flintrock"

        return {
            'InstanceGroups': get_genomic_instance_group_config(instance_type, instance_count),
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': self.subnet,
            'Ec2KeyName': ec2_key_name
        }

    def get_spark_config_file_path(self) -> str:
        return f'conf/spark-config-genomic.json'


def get_genomic_instance_group_config(instance_type: str, instance_count: int):
    return [
        {
            'Name': 'Core - 2',
            'Market': 'ON_DEMAND',
            'InstanceRole': 'CORE',
            "InstanceCount": instance_count,
            'InstanceType': instance_type,
            'EbsConfiguration': {
                'EbsBlockDeviceConfigs': [
                    {
                        'VolumeSpecification': {
                            'SizeInGB': 150,
                            'VolumeType': 'gp2'
                        },
                        'VolumesPerInstance': 8
                    }
                ]
            }
        },
        {
            'Name': 'Master - 1',
            'Market': 'ON_DEMAND',
            'InstanceRole': 'MASTER',
            'InstanceCount': 1,
            'InstanceType': "m5.4xlarge",
            'EbsConfiguration': {
                'EbsBlockDeviceConfigs': [
                    {
                        'VolumeSpecification': {
                            'SizeInGB': 128,
                            'VolumeType': 'gp2'
                        },
                        'VolumesPerInstance': 2
                    }
                ]
            }
        }
    ]
