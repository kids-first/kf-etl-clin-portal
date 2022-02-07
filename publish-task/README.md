# Publish task

This task will switch the alias in ElasticSearch.

## Prerequisites

ElasticSearch 7.X

## Task arguments (Mandatory)

- ElasticSearch url example: `http://localhost:9200`

- ElasticSearch port example: `9200`

- Release ID example: `RE_000001`

- Study IDs example: `SD_Z6MWD3H0;SD_Y6PXD3F0`

- Job type `study_centric` or `participant_centric` or `file_centric` or `biospecimen_centric`

Example: `http://localhost:9200 9200 RE_000004 SD_Z6MWD3H0;SD_65064P2Z file_centric`

Task will add index with name `[Job type]_[Study ID]_[Release ID]` to alias `[Job type]` and remove previous index for each Study ID from alias.