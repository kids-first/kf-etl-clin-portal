#!/bin/bash

aws s3 cp s3://kf-strides-232196027141-datalake-prd/reference_genome/GRCh38_full_analysis_set_plus_decoy_hla.fa \
/mnt/GRCh38_full_analysis_set_plus_decoy_hla.fa

aws s3 cp s3://kf-strides-232196027141-datalake-prd/reference_genome/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai \
/mnt/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai


