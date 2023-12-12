EMR Scripts
===========

These bunch of scripts can be used to :
- Run different ETL steps
- Start an EMR cluster with Zeppelin to debug data

## How to start EMR + Zeppelin debug cluster

Build docker image in this folder:
```
docker build -t runetl .
```

Authenticate with AWS:
```
igor awslogin
```

Run docker image. You need to mount volumes for your AWS credentials and for the code:
```bash
 docker run --rm -ti -v /Users/jecos/.aws:/root/.aws \
  -v /Users/jecos/workspace/kf-etl-clin-portal/bin:/root/work \
  -e AWS_PROFILE=Mgmt-Console-Devops-D3bCenter@232196027141 \
  -e AWS_REGION=us-east-1 \
  --entrypoint /bin/bash runetl
```

Then run the script to start the cluster:
```bash
bash-4.2# cd /root/work/
bash-4.2#  ./run_emr_zeppelin.sh --project kf-strides --environment prd --instance-type m5.8xlarge --instance-count 5 --bucket kf-strides-232196027141-datalake-prd --instance-profile kf-variant-emr-ec2-prd-profile --service-role kf-variant-emr-prd-role
flintrock
{
    "ClusterId": "j-XXXXXX",
    "ClusterArn": "arn:aws:elasticmapreduce:us-east-1:232196027141:cluster/j-XXXXXX"
}
```

