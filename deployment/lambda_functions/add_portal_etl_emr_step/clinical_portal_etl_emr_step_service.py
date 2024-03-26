from typing import Any

from portal_etl_emr_step_service import PortalEtlEmrStepService, get_next_step_prefix
from portal_emr_step_builder import EmrStepBuilder, EmrStepArgumentBuilder

# Default list of Portal ETL Steps
DEFAULT_CLINICAL_PORTAL_ETL_STEPS = ['cleanup jars', 'download and run fhavro-export', 'normalize dataservice',
                            'normalize clinical',
                            'enrich all', 'prepare index', 'index study', 'index participant', 'index file',
                            'index biospecimen']


class ClinicalPortalEtlEmrStepService(PortalEtlEmrStepService):
    def __init__(self, etl_args: dict):
        super(ClinicalPortalEtlEmrStepService, self).__init__(etl_args=etl_args)

    def get_default_etl_steps_to_execute(self) -> list:
        default_etl_steps_to_execute = list(DEFAULT_CLINICAL_PORTAL_ETL_STEPS)
        if self.etl_args['account'] == 'include':
            default_etl_steps_to_execute.remove('normalize dataservice')
        return default_etl_steps_to_execute

    def get_next_steps(self, portal_etl_steps_to_execute: list, current_etl_steps: list, study_ids: list) -> list:
        """
         Gets the next ETL step in the process.

         Returns:
             str: The next ETL step.
             :param study_ids:
             :param current_etl_steps:
             :param portal_etl_steps_to_execute:
         """
        next_step = get_next_step_prefix(portal_etl_steps_to_execute, current_etl_steps)
        if not next_step:
            return []
        return [next_step]


    def get_etl_step_description(self, next_etl_steps: list) -> list[Any]:
        elastic_search_endpoint = self.etl_args['esEndpoint']
        return [PORTAL_ETL_STEP_DESCRIPTION_MAP[next_etl_steps[0]](etl_config=self.etl_args,
                                                                   elastic_search_endpoint=elastic_search_endpoint,
                                                                   fhir_secret_object=self.fhir_secret_object)]

    def get_etl_current_step_names(self, current_etl_steps) -> list:
        return current_etl_steps


PORTAL_ETL_STEP_DESCRIPTION_MAP = {
    'cleanup jars': lambda etl_config, elastic_search_endpoint, fhir_secret_object:
    EmrStepBuilder("Cleanup jars", EmrStepArgumentBuilder()
                   .with_custom_args(["bash", "-c",
                                      "sudo rm -f /usr/lib/spark/jars/spark-avro.jar"])
                   .build()).build(),

    'download and run fhavro-export': lambda etl_config, elastic_search_endpoint, fhir_secret_object:
    EmrStepBuilder("Download and Run Fhavro-export", EmrStepArgumentBuilder()
                   .with_fhir_custom_job(etl_config['etlPortalBucket'], etl_config['input']['releaseId'],
                                         etl_config['input']['studyIds'], etl_config['input']['fhirUrl'],
                                         fhir_secret_object, etl_config['input']['verbose'])
                   .build()).build(),

    'normalize dataservice': lambda etl_config, elastic_search_endpoint, fhir_secret_object:
    EmrStepBuilder("Normalize Dataservice", EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.normalized.dataservice.RunNormalizeDataservice",
                                   etl_config['etlPortalBucket'])
                   .with_packages(["com.typesafe.play:play-ahc-ws-standalone_2.12:2.0.3"])
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=True)
                   .with_steps()
                   .with_release_id(etl_config['input']['releaseId'], include_flag=True)
                   .with_studies(etl_config['input']['studyIds'], include_flag=True)
                   .build()).build(),

    'normalize clinical': lambda etl_config, elastic_search_endpoint, fhir_secret_object:
    EmrStepBuilder("Normalize Clinical", EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.normalized.clinical.RunNormalizeClinical",
                                   etl_config['etlPortalBucket'])
                   .with_packages(["com.typesafe.play:play-ahc-ws-standalone_2.12:2.0.3"])
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=True)
                   .with_steps()
                   .with_release_id(etl_config['input']['releaseId'], include_flag=True)
                   .with_studies(etl_config['input']['studyIds'], include_flag=True)
                   .build()).build(),

    'enrich all': lambda etl_config, elastic_search_endpoint, fhir_secret_object:
    EmrStepBuilder("Enrich All", EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.enriched.clinical.RunEnrichClinical",
                                   etl_config['etlPortalBucket'])
                   .with_packages(["com.typesafe.play:play-ahc-ws-standalone_2.12:2.0.3"])
                   .with_custom_arg("all")
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=True)
                   .with_steps()
                   .with_studies(etl_config['input']['studyIds'], include_flag=True)
                   .build()).build(),

    'prepare index': lambda etl_config, elastic_search_endpoint, fhir_secret_object:
    EmrStepBuilder("Prepare Index", EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.prepared.clinical.RunPrepareClinical",
                                   etl_config['etlPortalBucket'])
                   .with_packages(["com.typesafe.play:play-ahc-ws-standalone_2.12:2.0.3"])
                   .with_custom_arg("all")
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=True)
                   .with_steps()
                   .with_studies(etl_config['input']['studyIds'], include_flag=True)
                   .build()).build(),

    'index study': lambda etl_config, elastic_search_endpoint, fhir_secret_object:
    EmrStepBuilder("Index Study", EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.indexed.clinical.RunIndexClinical",
                                   etl_config['etlPortalBucket'])
                   .with_packages(["org.elasticsearch:elasticsearch-spark-30_2.12:7.17.12"])
                   .with_custom_arg(elastic_search_endpoint)
                   .with_custom_arg("443")
                   .with_release_id(etl_config['input']['releaseId'], include_flag=False)
                   .with_studies(etl_config['input']['studyIds'], include_flag=False)
                   .with_custom_arg("study_centric")
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=False)
                   .build()).build(),

    'index participant': lambda etl_config, elastic_search_endpoint, fhir_secret_object:
    EmrStepBuilder("Index Participant", EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.indexed.clinical.RunIndexClinical",
                                   etl_config['etlPortalBucket'])
                   .with_packages(["org.elasticsearch:elasticsearch-spark-30_2.12:7.17.12"])
                   .with_custom_arg(elastic_search_endpoint)
                   .with_custom_arg("443")
                   .with_release_id(etl_config['input']['releaseId'], include_flag=False)
                   .with_studies(etl_config['input']['studyIds'], include_flag=False)
                   .with_custom_arg("participant_centric")
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=False)
                   .build()).build(),

    'index file': lambda etl_config, elastic_search_endpoint, fhir_secret_object:
    EmrStepBuilder("Index File", EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.indexed.clinical.RunIndexClinical",
                                   etl_config['etlPortalBucket'])
                   .with_packages(["org.elasticsearch:elasticsearch-spark-30_2.12:7.17.12"])
                   .with_custom_arg(elastic_search_endpoint)
                   .with_custom_arg("443")
                   .with_release_id(etl_config['input']['releaseId'], include_flag=False)
                   .with_studies(etl_config['input']['studyIds'], include_flag=False)
                   .with_custom_arg("file_centric")
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=False)
                   .build()).build(),

    'index biospecimen': lambda etl_config, elastic_search_endpoint, fhir_secret_object:
    EmrStepBuilder("Index Biospecimen", EmrStepArgumentBuilder()
                   .with_spark_job("bio.ferlab.etl.indexed.clinical.RunIndexClinical",
                                   etl_config['etlPortalBucket'])
                   .with_packages(["org.elasticsearch:elasticsearch-spark-30_2.12:7.17.12"])
                   .with_custom_arg(elastic_search_endpoint)
                   .with_custom_arg("443")
                   .with_release_id(etl_config['input']['releaseId'], include_flag=False)
                   .with_studies(etl_config['input']['studyIds'], include_flag=False)
                   .with_custom_arg("biospecimen_centric")
                   .with_config(etl_config['environment'], etl_config['account'], include_flag=False)
                   .build()).build(),
}

if __name__ == '__main__':
    test1 = 'Cleanup jars'
    test2 = 'Download and Run Fhavro-export'
    test3 = 'Prepare Index'
    test4 = 'Index Biospecimen'

    test_etl_config = {
        'environment': 'qa',
        'releaseId': 're_004',
        'etlVariantBucket': 'kf-strides-232196027141-datalake-qa',
        'instanceCount': 1,
        'clusterSize': 'large',
        'etlPortalBucket': 'bucket',
        'account': 'kf-strides',
        'esEndpoint': 'test',
        'input': {
            'test': 'test',
            'studyIds': ['SDTest1', 'SDTest2'],
            'releaseId': 're_1',
            "fhirUrl": "https://kf-api-fhir-service.kidsfirstdrc.org",
            "verbose": "true"
        },
        'portalEtlClusterId': '1234'
    }
    elastic_search_endpoint = 'test'
    clinical_step_service = ClinicalPortalEtlEmrStepService(test_etl_config)
    args = clinical_step_service.submit_next_portal_etl_step()
    print(args)
    args = clinical_step_service.submit_next_portal_etl_step()
    print(args)
    args = clinical_step_service.submit_next_portal_etl_step()
    print(args)
    args = clinical_step_service.submit_next_portal_etl_step()
    print(args)
