### Fhavro Export Task

The Fhavro Export task exports data from a [FHIR](https://hapifhir.io/) server, serialize the data into its respective [Apache Avro](https://avro.apache.org/) file using the library [Fhavro](https://github.com/Ferlab-Ste-Justine/fhavro) and write the said file unto an [Amazon S3](https://aws.amazon.com/s3/) bucket.

The format of the file(s) saved on Amazon S3 should be as followed:

```
raw/fhir/<resource-name>/study_id=<study-id>/release_id=<release-id>/<schema-name>.avro
```

Where:

- Resource-name: Name of the FHIR v4.0.1 resources
- Study-id: ID of the study
- Release-id: ID of the release  
- Schema-name: Name of the Avro schema used to serialize the data

## How to?

### Pre-requisites

To develop in a standalone local environment, you can install [Minio](https://docs.min.io/docs/minio-quickstart-guide.html) to run a standalone MinIO server as a container that simulates an S3 AWS environment.

Once installed for your specific environment, it should (by default) serve the console (the user interface) at a random port (we advise to add the console-address argument to pre-defined the port to use):
```
Unix: ./minio server ./data --console-address ":9001"
```
Therefore, the console is located at http://localhost:9001

The credentials is as follow;

Username: minioadmin<br>
Password: minioadmin

<b>Note: The credentials needs to be defined as an environment variable when launching the app.
This is to simulate the dockerized application</b>

### Environment Variables (Mandatory)

- `ENV` : The environment to read the configuration from
- `AWS_ACCESS_KEY` : The access key
- `AWS_SECRET_KEY` : The secret key
- `AWS_REGION`     : The region

Example of a setup in DEV with Minio for KidsFirst:
- ENV=KFDRC-DEV;
- AWS_REGION=us-east-1;
- AWS_ACCESS_KEY=minioadmin;
- AWS_SECRET_KEY=minioadmin;

### Arguments (Mandatory)

1st argument: release id

2nd argument: study ids separated by `;`

Example : `RE_000001 SD_Z6MWD3H0;SD_Y6PXD3F0`

### Launch the application?

The main application is located at: src/main/scala/bio/ferlab/fhir/etl/FhavroExport. Right-click and run.

### Fix Permission Issue?

As of right now, you need to provide your own cookie by modifying the keycloak section of the configuration file at /src/main/resources/application.conf
This should be fix in the future with Keycloak but for now this is what you have to do.

To retrieve your own cookie:

1. Click [here](https://kf-api-fhir-service.kidsfirstdrc.org/$export?_type=Patient)
2. Open the developer window (by default its F12 on most browser)
3. Search for a GET request which as the 400 BAD Request symbol
4. Go into the Headers section
5. Scroll down to the Request Headers section
6. Copy the value of the Cookie. WARNING: On Firefox, check the Raw toggle otherwise the value of the cookie is truncated.
7. Paste and replace the value in the configuration at keycloak-config.cookie.

### How to Package?

Open a terminal at the root of your project and simply execute the following command (or use your IDE to do so):
```
./build.sh
```

## Configuration and environment variables

The configuration is defined in file [application-ENV.conf](src/main/resources/application-kfdrc-dev.conf).
Some attributes can be overridden by environment variables. For instance :

The configuration itself is loaded based on the environment. In order to switch environment, you need to provide the following environment variable:

### AWS
- `AWS_ENDPOINT` : The url of the object store.

### FHIR
- `FHIR_URL` : Fhir Server URL

For each Resources in the FHIR configuration, the Schema needs to unique by type of resource