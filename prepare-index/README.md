# Prepare Index Task

## Environment Variables (Mandatory for local dev)

- `AWS_ACCESS_KEY` : The access key
- `AWS_SECRET_KEY` : The secret key
- `AWS_ENDPOINT`   : Endpoint to S3

Example of a setup with Minio:
- AWS_ENDPOINT=http://127.0.0.1:9000;
- AWS_ACCESS_KEY=minioadmin;
- AWS_SECRET_KEY=minioadmin;

## Arguments (Mandatory)

1st argument: Path to configuration file `config/qa.conf` or `config/prod.conf` or `config/dev.conf`

2nd argument: Steps to run for ETL `default`

3rd argument: Job type `study_centric` or `participant_centric` or `file_centric` or `biospecimen_centric` or `all`

4th argument: Release id

5th argument: Study ids separated by `,`

Example : `./config/dev.conf default study_centric RE_000001 SD_Z6MWD3H0;SD_Y6PXD3F0`

## Launch the application

The main application is located at: src/main/scala/bio/ferlab/fhir/etl/PrepareIndex. Right-click and run.
