#!/bin/bash

if [ "$1" == "" ]; then
    echo Usage $0 "<json-config>"
    exit 1
fi

cfg_file=$1

# set -x

# TODO parse from ../cymballic.json
export AWS_ACCOUNT_ID=058264346350

# TODO commonly disallow non [A-Za-z_]
CUSTOMER=$(cat $cfg_file | jq -r .customer)
GS_BUCKET_NAME=$(cat $cfg_file | jq -r .bucket_name)
TABLE_NAME=$(cat $cfg_file | jq -r .table_name)

cols=$(./infer-parquet-schema.sh $cfg_file | jq .)

cat <<EOF > /tmp/${TABLE_NAME}.def
{
    "Name": "${TABLE_NAME}",
    "StorageDescriptor": {
        "Columns": $cols,
        "Location": "gs://${GS_BUCKET_NAME}/${TABLE_NAME}/",
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        }
    },
    "TableType": "EXTERNAL_TABLE",
    "Parameters": {
        "classification": "parquet",
        "sourceFile": "${TABLE_NAME}.parquet"
    }
}
EOF

echo Wrote column definitions to /tmp/${TABLE_NAME}.def

aws glue --profile main-profile create-database \
  --catalog-id ${AWS_ACCOUNT_ID} \
  --database-input "{
    \"Name\": \"${CUSTOMER}\",
    \"Description\": \"Database for GCS data\",
    \"LocationUri\": \"google-cloud-storage-flag\"
}"

aws glue --profile main-profile create-table --database-name "${CUSTOMER}" --table-input "$(cat /tmp/${TABLE_NAME}.def)"
