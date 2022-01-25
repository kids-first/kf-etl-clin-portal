# Index task

This task read data from S3 (output of Prepare Index task) and write it to ElasticSearch.

## Prerequisites

ElasticSearch 7.X
S3/Minio

## Templates

Need to upload index templates (in resources) in S3 in `esindex/templates` directory

## Task arguments

- ElasticSearch url example: `http://localhost:9200`

- ElasticSearch port example: `9200`

- Release ID example: `RE_000001`

- Study IDs example: `SD_Z6MWD3H0;SD_Y6PXD3F0`

- Job type `study_centric` or `participant_centric` or `file_centric` or `biospecimen_centric`

- Path to configuration file `config/qa.conf` or `config/prod.conf` or `config/dev.conf`

Task will import data in index with name `[Job type]_[Study ID]_[Release ID]` in lowercase.