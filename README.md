<p align="center">
  <img src="docs/kids_first_logo.svg" alt="Kids First repository logo" width="660px" />
</p>

## ETL (Clinical && Genomic)

ETL that extracts clinical and genomic data for the KF data portal.

## Run the ETL via StepFunctions

The Clinical and Genomic ETLs can now be started via AWS Step Functions. When the ETL finishes a status will be reported 
to `kf-portal-etl-alerts` channel (depends on Include or KF)

* stateMachinePortalEtl-qa (Runs both Clinical and Genomic ETL within the QA env)
* stateMachineGenomicIndexEtl-qa (Runs Index Stage of Genomic ETL within the QA env)
* stateMachinePortalEtl-prd (Runs both Clinical and Genomic ETL within the PRD env)
* stateMachineGenomicIndexEtl-prd (Runs Index Stage of Genomic ETL within the PRD env)

### Clinical ETL
Example Input JSON
```json
{
  "releaseId": "re_20231009_1",
  "studyIds": [
    "SD_1P41Z782",
    "SD_ME0WME0W"
  ],
  "clusterSize": "small",
  "portalEtlName": "Clinical QA re_20231009_1",
  "fhirUrl": "http://app.sd-kf-api-fhir-service-qa.kf-strides.org:8000",
  "runGenomicEtl": false
}
```
* releaseId (required) - Release ID passed to Spark Jobs
* studyIds (required)  - List of Study Ids to run Portal ETL against
* fhirUrl (required) - FHIR Url used to Clinical Portal ETL
* clusterSize - Used to control EMR Cluster Size (Instance Count)
```python
CLUSTER_SIZE_TO_INSTANCE_COUNT = {
    'xsmall': 1,
    'small': 5,
    'medium': 10,
    'large': 20,
    'xlarge': 30
}
```
* portalEtlName - Overwrite Auto Generated Cluster Name
* runGenomicEtl - Switch for Clinical or Genomic ETL (Default: false)
* etlStepsToExecute - Specifies what steps to execute (Ex: `"etlStepsToExecute" : ["stepToExecuteA", "stepToExecuteB"]`)
```python
#Possible Values
CLINICAL_PORTAL_ETL_STEPS = ['cleanup jars', 'download and run fhavro-export', 'normalize dataservice',
                            'normalize clinical',
                            'enrich all', 'prepare index', 'index study', 'index participant', 'index file',
                            'index biospecimen']
```

### Genomic ETL
#### Note - The Genomic ETL will automatically kick off Genomic Index ETL once completed 
Example Input JSON
```json
{
  "releaseId": "re_TEST",
  "studyIds": [
    "SD_PREASA7S"
  ],
  "clusterSize": "small",
  "portalEtlName": "Genomic QA re_TEST",
  "runGenomicEtl": true,
  "chromosomes": [
    "all"
  ],
  "sample": "0.0001"
}
```
* releaseId (required) - Release ID passed to Spark Jobs
* studyIds (required)  - List of Study Ids to run Portal ETL against
* runGenomicEtl (required) - Switch for Clinical or Genomic ETL (will run Clinical ETL if not set to true)
* chromosomes - Specifies which chromosomes to run against (Passed Arg to Genomic Index ETL)
  * Warning!!! if argument `all` is passed then `sample` must be set
* sample - ex: "0.0001"
* etlStepsToExecute - Specifies what steps to execute (Ex: `"etlStepsToExecute" : ["stepToExecuteA", "stepToExecuteB"]`)
```python
#Possible Values
GENOMIC_PORTAL_ETL_STEPS = ['normalize-snv', 'normalize-consequences', 'enrich-variant', 'enrich-consequences',
                            'prepare-variant_centric',
                            'prepare-variant_suggestions', 'prepare-gene_centric', 'prepare-gene_suggestions']
```

### Genomic Index ETL
Example Input JSON
```json
{
  "releaseId": "re_TEST",
  "portalEtlName": "Genomic QA re_TEST",
}
```
* chromosomes - Specifies which chromosomes to run against (Passed Arg to Genomic Index ETL)
    * Warning!!! if argument `all` is passed then `sample` must be set
* sample - ex: "0.0001"
* etlStepsToExecute - Specifies what steps to execute (Ex: `"etlStepsToExecute" : ["stepToExecuteA", "stepToExecuteB"]`)
```python
#Possible Values
GENOMIC_INDEX_PORTAL_ETL_STEPS = ['variant_centric', 'variant_suggestions', 'gene_centric', 'gene_suggestions']
```
