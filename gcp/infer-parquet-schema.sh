#!/bin/bash

# TODO replace it with python

if [ "$1" == "" ]; then
    echo Usage $0 "<json-config>"
    exit 1
fi
cfg_file=$1

# set -x

GCS_ACCOUNT_NAME=$(cat $cfg_file | jq -r .customer)
SECRET_MGR_GCP_KEY_NAME=$(cat $cfg_file | jq -r .secret_mgr_gcp_key_name)
CUSTOMER=$(cat $cfg_file | jq -r .customer)
GS_BUCKET_NAME=$(cat $cfg_file | jq -r .bucket_name)
TABLE_NAME=$(cat $cfg_file | jq -r .table_name)

key=$(aws secretsmanager --profile main-profile \
	  get-secret-value --secret-id ${SECRET_MGR_GCP_KEY_NAME} | jq -r .SecretString)

echo $key > /tmp/${CUSTOMER}_key.json

export GOOGLE_APPLICATION_CREDENTIALS=/tmp/${CUSTOMER}_key.json
gcloud auth activate-service-account --key-file=/tmp/${CUSTOMER}_key.json

gcloud storage cp gs://${GS_BUCKET_NAME}/${TABLE_NAME}/${TABLE_NAME}.parquet /tmp/${TABLE_NAME}.parquet
cols=$(python3 infer-parquet-schema.py /tmp/${TABLE_NAME}.parquet)
echo $cols
