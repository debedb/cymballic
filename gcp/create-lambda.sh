#!/bin/bash

if [ "$1" == "" ]; then
    echo Usage $0 "<json-config>"
    exit 1
fi
cfg_file=$1

# TODO commonly disallow non [A-Za-z_]
CUSTOMER=$(cat $cfg_file | jq -r .customer)
SECRET_MGR_GCP_KEY_NAME=$(cat $cfg_file | jq -r .secret_mgr_gcp_key_name)
   
# set -x

# TODO parse from ../cymballic.json
export AWS_ACCOUNT_ID=058264346350

aws lambda --profile main-profile create-function \
  --function-name DlgGcsConnector_${CUSTOMER} \
  --package-type Image \
  --code ImageUri=${AWS_ACCOUNT_ID}.dkr.ecr.us-east-1.amazonaws.com/athena-gcs-connector:latest \
  --role arn:aws:iam::${AWS_ACCOUNT_ID}:role/cymballic-lambda-exec-role \
  --environment "Variables={spill_bucket=metadata,spill_prefix=gcs-spill-${CUSTOMER},SOURCE_TYPE=gcs,secret_manager_gcp_creds_name=${SECRET_MGR_GCP_KEY_NAME}}" \
  --memory-size 2048 \
  --timeout 900 \
  --ephemeral-storage Size=1024

aws athena --profile main-profile \
    create-data-catalog \
  --name ext_cat_gcs_${CUSTOMER} \
  --type LAMBDA \
  --description "Google Cloud Storage Connector" \
  --parameters function=arn:aws:lambda:us-east-1:${AWS_ACCOUNT_ID}:function:DlgGcsConnector_${CUSTOMER}

