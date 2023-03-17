# Config
set -e
RELEASE_ID=${1}
STUDY_ID="GIMS"
KF_BUCKET="s3://kf-strides-232196027141-datalake-qa/jobs"
REGION=us-west-2
ES_ENDPOINT="https://vpc-genetics-info-commons-es-xz26u5sd5zjg2umnzyis6na5gq.${REGION}.es.amazonaws.com"
FHIR_SERVER=http://10.90.172.42:443/fhir
S3_BUCKET="d3b-portal-65-4-r-us-west-2.sec.ucsf.edu"

# Copy jars from KF S3 bucket to here
export AWS_PROFILE="strides"
aws s3 cp ${KF_BUCKET}/fhavro-export.jar work-dir/fhavro-export.jar
aws s3 cp ${KF_BUCKET}/import-task.jar work-dir/import-task.jar
aws s3 cp ${KF_BUCKET}/enrich.jar work-dir/enrich.jar
aws s3 cp ${KF_BUCKET}/prepare-index.jar work-dir/prepare-index.jar
aws s3 cp ${KF_BUCKET}/index-task.jar work-dir/index-task.jar
aws s3 cp ${KF_BUCKET}/publish-task.jar work-dir/publish-task.jar
unset AWS_PROFILE

# Check spark conf are here
if [[ ! -f $(pwd)/conf/spark-defaults.conf ]]; then
    echo "Missing spark-defaults.conf file"
    exit 1
fi

# Execute fhavro-export
docker run -it -e FHIR_URL=${FHIR_SERVER} -e AWS_REGION=${REGION} -e BUCKET=${S3_BUCKET} --rm -v $(pwd):/app amazoncorretto:11 java -jar /app/fhavro-export.jar ${RELEASE_ID} ${STUDY_ID} default

# Execute import-task
docker run -it --rm \
-v /root/kf-etl-clin-portal/bin/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v /root/kf-etl-clin-portal/bin/work-dir/ivy:/tmp/ivy \
-v /root/kf-etl-clin-portal/bin/work-dir:/opt/spark/work-dir \
-e AWS_REGION=${REGION} \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" --class bio.ferlab.fhir.etl.ImportTask import-task.jar config/ucsf.conf default ${RELEASE_ID} ${STUDY_ID}

# Execute enrich-task
docker run -it --rm \
-v /root/kf-etl-clin-portal/bin/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v /root/kf-etl-clin-portal/bin/work-dir/ivy:/tmp/ivy \
-v /root/kf-etl-clin-portal/bin/work-dir:/opt/spark/work-dir \
-e AWS_REGION=${REGION} \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" --class bio.ferlab.enrich.etl.Enrich enrich.jar config/ucsf.conf default ${STUDY_ID}

# Execute prepare-index
docker run -it --rm \
-v /root/kf-etl-clin-portal/bin/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v /root/kf-etl-clin-portal/bin/work-dir/ivy:/tmp/ivy \
-v /root/kf-etl-clin-portal/bin/work-dir:/opt/spark/work-dir \
-e AWS_REGION=${REGION} \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" --class bio.ferlab.fhir.etl.PrepareIndex prepare-index.jar config/ucsf.conf default all ${STUDY_ID}

# Execute index-task for study_centric
docker run -it --rm \
-v /root/kf-etl-clin-portal/bin/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v /root/kf-etl-clin-portal/bin/work-dir/ivy:/tmp/ivy \
-v /root/kf-etl-clin-portal/bin/work-dir:/opt/spark/work-dir \
-e AWS_REGION=${REGION} \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" --class bio.ferlab.fhir.etl.IndexTask index-task.jar ${ES_ENDPOINT} 443 ${RELEASE_ID} ${STUDY_ID} study_centric config/ucsf.conf

# Execute index-task for participant_centric
docker run -it --rm \
-v /root/kf-etl-clin-portal/bin/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v /root/kf-etl-clin-portal/bin/work-dir/ivy:/tmp/ivy \
-v /root/kf-etl-clin-portal/bin/work-dir:/opt/spark/work-dir \
-e AWS_REGION=${REGION} \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" --class bio.ferlab.fhir.etl.IndexTask index-task.jar ${ES_ENDPOINT} 443 ${RELEASE_ID} ${STUDY_ID} participant_centric config/ucsf.conf

# Execute index-task for file_centric
docker run -it --rm \
-v /root/kf-etl-clin-portal/bin/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v /root/kf-etl-clin-portal/bin/work-dir/ivy:/tmp/ivy \
-v /root/kf-etl-clin-portal/bin/work-dir:/opt/spark/work-dir \
-e AWS_REGION=${REGION} \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" --class bio.ferlab.fhir.etl.IndexTask index-task.jar ${ES_ENDPOINT} 443 ${RELEASE_ID} ${STUDY_ID} file_centric config/ucsf.conf

# Execute index-task for biospecimen_centric
docker run -it --rm \
-v /root/kf-etl-clin-portal/bin/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v /root/kf-etl-clin-portal/bin/work-dir/ivy:/tmp/ivy \
-v /root/kf-etl-clin-portal/bin/work-dir:/opt/spark/work-dir \
-e AWS_REGION=${REGION} \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" --class bio.ferlab.fhir.etl.IndexTask index-task.jar ${ES_ENDPOINT} 443 ${RELEASE_ID} ${STUDY_ID} biospecimen_centric config/ucsf.conf

# Execute publish-task
docker run -it --rm -v $(pwd):/app amazoncorretto:11 java -jar /app/publish-task.jar ${ES_ENDPOINT} 443 ${RELEASE_ID} ${STUDY_ID} all
