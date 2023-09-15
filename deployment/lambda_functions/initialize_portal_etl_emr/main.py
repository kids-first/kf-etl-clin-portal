from genomic_portal_etl_emr_creator import GenomicPortalEtlEmrCreator
from clinical_portal_etl_emr_creator import ClinicalPortalEtlEmrCreator


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
    etl_user_input = etl_args['input']
    run_genomic_etl = etl_user_input.get('runGenomicEtl', False)

    portal_etl_emr_creator = GenomicPortalEtlEmrCreator(etl_args) if run_genomic_etl else ClinicalPortalEtlEmrCreator(
        etl_args)
    etl_args['portalEtlClusterId'] = portal_etl_emr_creator.create_emr()
    return etl_args


if __name__ == '__main__':
    test_event = {
        'environment': 'qa',
        'releaseId': 're_004',
        'input': {

                "releaseId": "re_TEST",
                "studyIds": ["SD_Y6VRG6MD"],
                "clusterSize": "medium",
                "portalEtlName": "TEST_GENOMIC",
                "fhirUrl": "http://app.sd-kf-api-fhir-service-qa.kf-strides.org:8000",
                "runGenomicEtl": 'true'

        },
        'etlPortalBucket': 'kf-strides-232196027141-datalake-qa',
        'instanceCount': 1,
        'instanceProfile': 'my-instance-profile',
        'clusterSize': 'large',
        'emrInstanceProfile': 'kf-variant-emr-ec2-qa-profile',
        'emrServiceRole': 'kf-variant-emr-qa-role',
        'emrEc2Subnet': 1234,
        "runGenomicEtl": 'true',
        'project': 'kf-strides',
    }

    initialize_portal_etl_emr(test_event, None)
