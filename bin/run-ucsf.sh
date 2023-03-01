# Config
release_id=${1}
aws_profile=${2}
study_id = "???" #FIXME
kidsfirst_bucket="s3://kf-strides-232196027141-datalake-qa/jobs"
es_endpoint= "???" #FIXME

# Copy jars from KF S3 bucket to here
aws s3 cp ${kidsfirst_bucket}/fhavro-export.jar fhavro-export.jar
aws s3 cp ${kidsfirst_bucket}/import-task.jar import-task.jar
aws s3 cp ${kidsfirst_bucket}/prepare-index.jar prepare-index.jar
aws s3 cp ${kidsfirst_bucket}/index-task.jar index-task.jar
aws s3 cp ${kidsfirst_bucket}/publish-task.jar publish-task.jar

# Check spark conf are here
[ ! -f $(pwd)/conf/spark-defaults.conf ] && exit 1

# Execute fhavro-export
$JAVA_HOME/bin/java -jar fhavro-export.jar ${release_id} ${study_id} default

# Execute import-task
docker run -it --rm \
-v $(pwd)/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v $(pwd)/spark_ivy:/opt/spark/.ivy2 \
-v $HOME/.aws:/home/185/.aws \
-e AWS_PROFILE=${aws_profile} \
-e AWS_REGION=us-east-1 \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --class bio.ferlab.fhir.etl.ImportTask import-task.jar config/ucsf.conf default ${release_id} ${study_id}

# Execute prepare-index
docker run -it --rm \
-v $(pwd)/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v $(pwd)/spark_ivy:/opt/spark/.ivy2 \
-v $HOME/.aws:/home/185/.aws \
-e AWS_PROFILE=${aws_profile} \
-e AWS_REGION=us-east-1 \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --class bio.ferlab.fhir.etl.PrepareIndex prepare-index.jar config/ucsf.conf default all ${release_id} ${study_id}

# Execute index-task for study_centric
docker run -it --rm \
-v $(pwd)/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v $(pwd)/spark_ivy:/opt/spark/.ivy2 \
-v $HOME/.aws:/home/185/.aws \
-e AWS_PROFILE=${aws_profile} \
-e AWS_REGION=us-east-1 \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --class bio.ferlab.fhir.etl.IndexTask index-task.jar ${es_endpoint} 443 ${release_id} ${study_id} study_centric config/ucsf.conf

# Execute index-task for participant_centric
docker run -it --rm \
-v $(pwd)/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v $(pwd)/spark_ivy:/opt/spark/.ivy2 \
-v $HOME/.aws:/home/185/.aws \
-e AWS_PROFILE=${aws_profile} \
-e AWS_REGION=us-east-1 \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --class bio.ferlab.fhir.etl.IndexTask index-task.jar ${es_endpoint} 443 ${release_id} ${study_id} participant_centric config/ucsf.conf

# Execute index-task for file_centric
docker run -it --rm \
-v $(pwd)/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v $(pwd)/spark_ivy:/opt/spark/.ivy2 \
-v $HOME/.aws:/home/185/.aws \
-e AWS_PROFILE=${aws_profile} \
-e AWS_REGION=us-east-1 \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --class bio.ferlab.fhir.etl.IndexTask index-task.jar ${es_endpoint} 443 ${release_id} ${study_id} file_centric config/ucsf.conf

# Execute index-task for biospecimen_centric
docker run -it --rm \
-v $(pwd)/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
-v $(pwd)/spark_ivy:/opt/spark/.ivy2 \
-v $HOME/.aws:/home/185/.aws \
-e AWS_PROFILE=${aws_profile} \
-e AWS_REGION=us-east-1 \
-p 4040:4040 apache/spark:3.3.1 \
/opt/spark/bin/spark-submit --deploy-mode client --class bio.ferlab.fhir.etl.IndexTask index-task.jar ${es_endpoint} 443 ${release_id} ${study_id} biospecimen_centric config/ucsf.conf

# Execute publish-task
$JAVA_HOME/bin/java -jar publish-task.jar ${es_endpoint} 443 ${release_id} ${study_id} all
