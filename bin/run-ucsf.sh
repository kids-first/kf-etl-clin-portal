set -e
# Config
RELEASE_ID=${1}
STUDY_ID="GIMS"
KF_BUCKET="s3://kf-strides-232196027141-datalake-qa/jobs"
ES_ENDPOINT="https://vpc-genetics-info-commons-es-xz26u5sd5zjg2umnzyis6na5gq.us-west-2.es.amazonaws.com"
FHIR_URL=http://10.90.172.42:443
REGION=us=west-2

# Copy jars from KF S3 bucket to here
aws s3 cp ${KF_BUCKET}/fhavro-export.jar fhavro-export.jar
aws s3 cp ${KF_BUCKET}/import-task.jar import-task.jar
aws s3 cp ${KF_BUCKET}/prepare-index.jar prepare-index.jar
aws s3 cp ${KF_BUCKET}/index-task.jar index-task.jar
aws s3 cp ${KF_BUCKET}/publish-task.jar publish-task.jar
aws s3 cp ${KF_BUCKET}/enrich.jar enrich.jar

# Check spark conf are here
if [[ ! -f $(pwd)/conf/spark-defaults.conf ]]; then
    echo "Missing spark-defaults.conf file"
    exit 1
fi

# Execute fhavro-export
docker run -it --rm -e FHIR_URL=${FHIR_URL} -e AWS_REGION=${REGION} -v $(pwd):/app amazoncorretto:11 java -jar /app/fhavro-export.jar ${RELEASE_ID} ${STUDY_ID} default

# Execute import-task
docker run -it --rm \
-v $(pwd)/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v $(pwd)/spark_ivy:/opt/spark/.ivy2 \
-v $(pwd):/opt/spark/work-dir \
-e AWS_REGION=${REGION} \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --class bio.ferlab.fhir.etl.ImportTask import-task.jar config/ucsf.conf default ${RELEASE_ID} ${STUDY_ID}

# Execute prepare-index
docker run -it --rm \
-v $(pwd)/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v $(pwd)/spark_ivy:/opt/spark/.ivy2 \
-v $(pwd):/opt/spark/work-dir \
-e AWS_REGION=${REGION} \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --class bio.ferlab.fhir.etl.PrepareIndex prepare-index.jar config/ucsf.conf default all ${RELEASE_ID} ${STUDY_ID}

# Execute index-task for study_centric
docker run -it --rm \
-v $(pwd)/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v $(pwd)/spark_ivy:/opt/spark/.ivy2 \
-v $(pwd):/opt/spark/work-dir \
-e AWS_REGION=${REGION} \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --class bio.ferlab.fhir.etl.IndexTask index-task.jar ${ES_ENDPOINT} 443 ${RELEASE_ID} ${STUDY_ID} study_centric config/ucsf.conf

# Execute index-task for participant_centric
docker run -it --rm \
-v $(pwd)/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v $(pwd)/spark_ivy:/opt/spark/.ivy2 \
-v $(pwd):/opt/spark/work-dir \
-e AWS_REGION=${REGION} \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --class bio.ferlab.fhir.etl.IndexTask index-task.jar ${ES_ENDPOINT} 443 ${RELEASE_ID} ${STUDY_ID} participant_centric config/ucsf.conf

# Execute index-task for file_centric
docker run -it --rm \
-v $(pwd)/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v $(pwd)/spark_ivy:/opt/spark/.ivy2 \
-v $(pwd):/opt/spark/work-dir \
-e AWS_REGION=${REGION} \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --class bio.ferlab.fhir.etl.IndexTask index-task.jar ${ES_ENDPOINT} 443 ${RELEASE_ID} ${STUDY_ID} file_centric config/ucsf.conf

# Execute index-task for biospecimen_centric
docker run -it --rm \
-v $(pwd)/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v $(pwd)/spark_ivy:/opt/spark/.ivy2 \
-v $(pwd):/opt/spark/work-dir \
-e AWS_REGION=${REGION} \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --class bio.ferlab.fhir.etl.IndexTask index-task.jar ${ES_ENDPOINT} 443 ${RELEASE_ID} ${STUDY_ID} biospecimen_centric config/ucsf.conf

# Execute publish-task
docker run -it --rm -v $(pwd):/app amazoncorretto:11 java -jar /app/publish-task.jar ${ES_ENDPOINT} 443 ${RELEASE_ID} ${STUDY_ID} all
