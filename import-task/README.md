# Import Task

## Generate configuration file

The config generator is located at: src/main/scala/bio/ferlab/fhir/etl/config/ConfigurationGenerator. Right-click and run.

## Environment Variables (Mandatory)

- `AWS_ACCESS_KEY` : The access key
- `AWS_SECRET_KEY` : The secret key
- `AWS_REGION`     : The region

Example of a setup with Minio:
- AWS_REGION=us-east-1;
- AWS_ACCESS_KEY=minioadmin;
- AWS_SECRET_KEY=minioadmin;

## Arguments (Mandatory)

1st argument: Path to configuration file `config/qa.conf` or `config/prod.conf` or `config/dev.conf`

2nd argument: Steps to run for ETL `default`

3rd argument: Release id

4th argument: Study ids separated by `;`

Example : `./config/dev.conf default RE_000001 SD_Z6MWD3H0;SD_Y6PXD3F0`

## Launch the application

The main application is located at: src/main/scala/bio/ferlab/fhir/etl/ImportTask. Right-click and run.
