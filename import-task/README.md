# Import Task

## Generate configuration file

The config generator is located at: src/main/scala/bio/ferlab/fhir/etl/config/ConfigurationGenerator. Right-click and run.

## Environment Variables (Mandatory for local dev)

- `AWS_ACCESS_KEY` : The access key
- `AWS_SECRET_KEY` : The secret key
- `AWS_ENDPOINT`   : Endpoint to S3

Example of a setup with Minio:
- AWS_ENDPOINT=http://127.0.0.1:9000;
- AWS_ACCESS_KEY=minioadmin;
- AWS_SECRET_KEY=minioadmin;

## Arguments (Mandatory)

1st argument: Path to configuration file `config/qa-[project].conf` or `config/prod-[project].conf` or `config/dev-[project].conf`

2nd argument: Steps to run for ETL `default`

3rd argument: Release id

4th argument: Study ids separated by `,`

Example : `./config/dev-[project].conf default RE_000001 SD_Z6MWD3H0;SD_Y6PXD3F0`

## Launch the application

The main application is located at: src/main/scala/bio/ferlab/fhir/etl/ImportTask. Right-click and run.
