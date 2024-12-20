import argparse
import boto3
import json
import os
import time
import psycopg2
import pandas as pd
import logging
from botocore.exceptions import ClientError, UnauthorizedSSOTokenError
import configparser

AWS_REGION = 'us-east-1'

SEPARATOR = "=" * 80

# TODO: Consider moving bucket_name and database_name generation to config file

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def fail_fast(msg):
    logger.error(msg)
    exit(1)

def ensure_sso_session(profile):
    session = boto3.Session(profile_name=profile)
    try:
        # Try to use the session to verify it's valid
        session.client('sts').get_caller_identity()
        logger.info(f"Using existing AWS SSO session for profile {profile}")
        return session, get_account_id(profile)
    except UnauthorizedSSOTokenError:
        fail_fast(f"AWS SSO session has expired or is invalid for profile '{profile}'.\n"
                 f"Please run: aws sso login --profile {profile} --no-browser")
    except ClientError:
        fail_fast(f"No active session for profile {profile}. Please run: aws sso login --profile {profile} --no-browser")
    return session, get_account_id(profile)

def get_account_id(profile):
    try:
        config = configparser.ConfigParser()
        config.read(os.path.expanduser('~/.aws/config'))
        profile_section = f"profile {profile}"
        if profile_section in config:
            return config[profile_section].get('sso_account_id')
    except Exception as e:
        logger.warning(f"Could not read account ID from AWS config: {e}")
    return None

def load_cymballic_config():
    with open('cymballic.json', 'r') as file:
        config = json.load(file)
        return config


def ensure_s3_bucket(session, bucket_name):

    s3 = session.client('s3', region_name=AWS_REGION)
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} already exists.")
    except ClientError:
        logger.info(f"Creating bucket {bucket_name} in {AWS_REGION}")
        try:
            if AWS_REGION == 'us-east-1':
                s3.create_bucket(Bucket=bucket_name)
            else:
                s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': AWS_REGION})
            logger.info(f"Created bucket {bucket_name}")
        except ClientError as e:
            if 'BucketAlreadyExists' not in str(e) and 'BucketAlreadyOwnedByYou' not in str(e):
                fail_fast(f"Failed to create bucket: {str(e)}")

def export_table_to_s3_parquet(rds_info, table_name, bucket_name, session):
    start_time = time.time()
    try:
        conn = psycopg2.connect(
            host=rds_info['host'],
            port=rds_info.get('port', 5432),
            dbname=rds_info['database'],
            user=rds_info['username'],
            password=rds_info['password']
        )
        logger.info("Successfully connected to the database.")
    except Exception as e:
        fail_fast(f"Failed to connect to database: {str(e)}")

    try:
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, conn)
        
        # Get column types from DataFrame
        column_types = df.dtypes.apply(lambda x: str(x)).to_dict()
        columns = [{"Name": col, "Type": "string" if "object" in typ else "double" if "float" in typ else "bigint" if "int" in typ else "string"} 
                  for col, typ in column_types.items()]
        
        parquet_path = f"/tmp/{table_name}.parquet"
        df.to_parquet(parquet_path, engine="pyarrow", index=False)
        os.makedirs("out", exist_ok=True)
        elapsed_time = time.time() - start_time
        logger.info(f"Exported data from table {table_name} to {parquet_path} in {int(elapsed_time/60)}m {int(elapsed_time%60)}s.")
    except Exception as e:
        fail_fast(f"Failed to export data: {str(e)}")
    finally:
        conn.close()

    s3 = session.client('s3', region_name=AWS_REGION)
    s3_key = f"{table_name}/{table_name}.parquet"
    upload_start = time.time()
    try:
        s3.upload_file(parquet_path, bucket_name, s3_key)
        upload_time = time.time() - upload_start
        logger.info(f"Uploaded data to s3://{bucket_name}/{s3_key} in {int(upload_time/60)}m {int(upload_time%60)}s")
    except Exception as e:
        fail_fast(f"Failed to upload file to S3: {str(e)}")

    return columns

def verify_parquet_exists(session, bucket_name, table_name):
    s3 = session.client('s3', region_name=AWS_REGION)
    try:
        s3.head_object(Bucket=bucket_name, Key=f"{table_name}/{table_name}.parquet")
        logger.info(f"Verified parquet file exists at s3://{bucket_name}/{table_name}/{table_name}.parquet")
        return True
    except ClientError:
        fail_fast(f"Parquet file not found at s3://{bucket_name}/{table_name}/{table_name}.parquet")

def create_glue_table(session, database_name, table_name, bucket_name, columns=None):
    glue = session.client('glue', region_name=AWS_REGION)
    
    # Create database if it doesn't exist
    try:
        glue.create_database(DatabaseInput={'Name': database_name})
        logger.info(f"Created Glue database {database_name}")
    except ClientError as e:
        if 'AlreadyExistsException' not in str(e):
            fail_fast(f"Failed to create Glue database: {str(e)}")
        logger.info(f"Glue database {database_name} already exists")

    def read_schema_using_pandas(bucket_name, table_name):
        # TODO is the best way to infer schema actually?
        # pyarrow? pandas? fastparquet
        # pyarrow/fastparquet appears to be using schema; pandas analyzing it
        try:
            s3_path = f"s3://{bucket_name}/{table_name}/{table_name}.parquet"
            logger.info(f"Attempting to read schema using pandas from {s3_path}")
            df = pd.read_parquet(s3_path)
            column_types = df.dtypes.apply(lambda x: str(x)).to_dict()
            columns = [{"Name": col, "Type": "string" if "object" in typ else "double" if "float" in typ else "bigint" if "int" in typ else "string"} 
                      for col, typ in column_types.items()]
            logger.info("Successfully inferred schema using pandas")
            return columns
        except Exception as e:
            logger.info(f"Could not read schema using pandas: {str(e)}")
            return None

    def read_schema_using_boto3(bucket_name, table_name):
        try:
            s3 = session.client('s3')
            # TODO find where this is not the case and why
            logger.info(f"Attempting to read schema using boto3 select_object_content from s3://{bucket_name}/{table_name}/{table_name}.parquet")
            response = s3.select_object_content(
                Bucket=bucket_name,
                Key=f"{table_name}/{table_name}.parquet",
                ExpressionType='SQL',
                Expression='SELECT * FROM s3object LIMIT 1',
                InputSerialization={'Parquet': {}},
                OutputSerialization={'JSON': {}}
            )
            
            for event in response['Payload']:
                if 'Records' in event:
                    record = json.loads(event['Records']['Payload'].decode('utf-8'))
                    columns = [{"Name": col, "Type": "string"} for col in record.keys()]
                    logger.info("Successfully inferred schema using boto3")
                    return columns
        except Exception as e:
            logger.info(f"Could not read schema using boto3: {str(e)}")
            return None

    # If columns not provided, infer from existing parquet file
    if not columns:
        columns = read_schema_using_pandas(bucket_name, table_name)
        if not columns:
            columns = read_schema_using_boto3(bucket_name, table_name)
        if not columns:
            logger.info("Could not read schema using pandas or boto3 (this is expected in cross-account setups). Creating table with empty schema to let Glue infer it.")
            columns = []

    table_input = {
        "Name": table_name,
        "StorageDescriptor": {
            "Columns": columns,
            "Location": f"s3://{bucket_name}/{table_name}/",
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Compressed": True,
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {}
            }
        },
        "TableType": "EXTERNAL_TABLE"
    }

    try:
        glue.create_table(DatabaseName=database_name, TableInput=table_input)
        logger.info(f"Created Glue table {table_name}")
    except ClientError as e:
        if 'AlreadyExistsException' in str(e):
            glue.update_table(DatabaseName=database_name, TableInput=table_input)
            logger.info(f"Updated existing Glue table {table_name}")
        else:
            fail_fast(f"Failed to create/update Glue table: {str(e)}")

def setup_permissions(session, source_account_id, database_name, bucket_name):
    cymballic_config = load_cymballic_config()
    timestamp = time.strftime("%Y%m%d_%H%M")
    log_dir = f"log/run_{timestamp}"
    os.makedirs(log_dir, exist_ok=True)

    def save_policy(policy_dict, policy_name):
        policy_file = f"{log_dir}/{policy_name}.json"
        with open(policy_file, "w") as f:
            json.dump(policy_dict, f, indent=2)
    target_account = cymballic_config.get('aws_account_id')
    target_service_role = cymballic_config.get('iam_service_role')
    target_sso_role = cymballic_config.get("iam_sso_role")

    s3 = session.client('s3', region_name=AWS_REGION)

    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    f"arn:aws:iam::{target_account}:role/{target_service_role}",
                    f"arn:aws:iam::{target_account}:role/{target_sso_role}"
                ]
            },
            "Action": ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"],
            "Resource": [
                f"arn:aws:s3:::{bucket_name}",
                f"arn:aws:s3:::{bucket_name}/*"
            ]
        }]
    }
    save_policy(bucket_policy, f"s3_bucket_policy_{bucket_name}")

    try:
        s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(bucket_policy))
        logger.info(f"Updated bucket policy for {bucket_name}")
    except ClientError as e:
        fail_fast(f"Failed to update bucket policy for {bucket_name}. Policy saved at {log_dir}/s3_bucket_policy_{bucket_name}.json\nError: {str(e)}")


    # Set up Glue policy
    glue = session.client('glue', region_name=AWS_REGION)
    target_resources = [
        f"arn:aws:glue:{AWS_REGION}:{source_account_id}:catalog",
        f"arn:aws:glue:{AWS_REGION}:{source_account_id}:database/{database_name}",
        f"arn:aws:glue:{AWS_REGION}:{source_account_id}:table/{database_name}/*"
    ]
    
    glue_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": [
                        f"arn:aws:iam::{target_account}:root",
                        f"arn:aws:iam::{target_account}:role/{target_service_role}",
                        f"arn:aws:iam::{target_account}:role/{target_sso_role}"
                    ]
                },
                "Action": [
                    "glue:*"
                ],
                "Resource": target_resources
            },
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "ram.amazonaws.com"
                },
                "Action": "glue:ShareResource",
                "Resource": target_resources
            }
        ]
    }
    save_policy(glue_policy, f"glue_policy_{database_name}")

    try:
        glue.put_resource_policy(PolicyInJson=json.dumps(glue_policy))
        logger.info("Updated Glue resource policy")
    except ClientError as e:
        fail_fast(f"Failed to update Glue policy for {database_name}. Policy saved at {log_dir}/glue_policy_{database_name}.json\nError: {str(e)}")

def main():
    start_time = time.time()
    parser = argparse.ArgumentParser(description="Onboard data to Athena")
    parser.add_argument("-c", "--config", required=True, help="Path to JSON configuration file")
    parser.add_argument("-t", "--table", required=True, help="Table name")
    args = parser.parse_args()

    cymballic_config = load_cymballic_config()
    global AWS_REGION
    AWS_REGION = cymballic_config.get('aws_region', 'us-east-1')
    try:
        with open(args.config, 'r') as file:
            config = json.load(file)
    except Exception as e:
        fail_fast(f"Failed to load configuration: {str(e)}")

    data_type = config.get("type")

    profile = config.get("aws_profile")
    customer = config.get("customer")
    bucket_name = customer.lower()
    database_name = customer.lower()

    session, source_account_id = ensure_sso_session(profile)
    if not source_account_id:
        fail_fast("Could not determine AWS account ID from profile configuration")

    if data_type == "postgres":
        ensure_s3_bucket(session, bucket_name)
        columns = export_table_to_s3_parquet(config, args.table, bucket_name, session)
        create_glue_table(session, database_name, args.table, bucket_name, columns)
    elif data_type == "parquet":
        verify_parquet_exists(session, bucket_name, args.table)
        create_glue_table(session, database_name, args.table, bucket_name)
    else:
        fail_fast(f"Unknown data type {data_type}")

    setup_permissions(session, source_account_id, database_name, bucket_name)
    
    total_time = time.time() - start_time
    logger.info(f"\nData onboarding completed successfully in {int(total_time/60)}m {int(total_time%60)}s!")
    cymballic_config = load_cymballic_config()
    logger.info(f"Please run:\n1. aws sso login --profile {cymballic_config['aws_account_profile']}\n2. python update.py -c {args.config}")

if __name__ == "__main__":
    main()
