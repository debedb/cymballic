import boto3
import json
import logging
import configparser
import time
import argparse
import configparser
import os

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def fail_fast(msg):
    logger.error(msg)
    exit(1)

# TODO: Consider extracting common AWS profile and config handling into a shared module
# TODO: Consider moving catalog naming convention to config file

def load_existing_policy(iam, role_name):
    try:
        response = iam.get_role_policy(RoleName=role_name, PolicyName=f"{role_name}-policy")
        return response['PolicyDocument']
    except iam.exceptions.NoSuchEntityException:
        return {
            "Version": "2012-10-17",
            "Statement": []
        }

def load_cymballic_config():
    with open('cymballic.json', 'r') as file:
        config = json.load(file)
        return config


def update_policy(source_profile, customer):
    # Load cymballic config to get target account profile
    cymballic_config = load_cymballic_config()
    aws_region = cymballic_config['aws_region']
    target_profile = cymballic_config['aws_account_profile']

    session = boto3.Session(profile_name=target_profile)
    iam = session.client('iam')
    role_name = cymballic_config['iam_service_role']

    # Get source account ID from AWS config
    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/config'))
    profile_section = f"profile {source_profile}"
    
    if profile_section not in config:
        fail_fast(f"Profile {source_profile} not found in ~/.aws/config")
    
    source_account_id = config[profile_section].get('sso_account_id')
    database_name = customer.lower()
    bucket_name = customer.lower()
    
    if not source_account_id:
        fail_fast(f"Could not find sso_account_id for profile {source_profile}")
    
    timestamp = time.strftime("%Y%m%d_%H%M")
    log_dir = f"log/run_{timestamp}"
    os.mkdir(log_dir)
    logger.info(f"Using source account ID: {source_account_id}")

    # Load existing policy
    policy = load_existing_policy(iam, role_name)
    
    # Create new statements for this database/bucket
    new_statements = [
        {
            "Effect": "Allow",
            "Action": [
                "glue:*",
                "sts:AssumeRole"
            ],
            "Resource": [
                f"arn:aws:glue:{aws_region}:{source_account_id}:catalog",
                f"arn:aws:glue:{aws_region}:{source_account_id}:database/{database_name}",
                f"arn:aws:glue:{aws_region}:{source_account_id}:table/{database_name}/*",
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:PutObject"
            ],
            "Resource": [
                f"arn:aws:s3:::{bucket_name}",
                f"arn:aws:s3:::{bucket_name}/*"
            ]
        }
    ]

    # Remove any existing statements for this database/bucket
    if 'Statement' in policy:
        policy['Statement'] = [
            stmt for stmt in policy['Statement']
            if not (
                any(res.endswith(f":database/{database_name}") for res in stmt.get('Resource', []))
                or any(bucket_name in res for res in stmt.get('Resource', []))
            )
        ]
    else:
        policy['Statement'] = []

    # Add new statements
    policy['Statement'].extend(new_statements)

    # Save policy for reference
    policy_path = f"{log_dir}/{role_name}-policy.json"
    with open(policy_path, "w") as f:
        json.dump(policy, f, indent=4)
    logger.info(f"Policy saved to {policy_path}")

    # Update role policy
    try:
        iam.put_role_policy(
            RoleName=role_name,
            PolicyName=f"{role_name}-policy",
            PolicyDocument=json.dumps(policy)
        )
        logger.info(f"Successfully updated policy for role {role_name}")
    except Exception as e:
        fail_fast(f"Failed to update role policy. Policy saved at {policy_path}\nError: {str(e)}")

def register_glue_catalog(source_profile, customer):
    cymballic_config = load_cymballic_config()
    target_profile = cymballic_config['aws_account_profile']
    session = boto3.Session(profile_name=target_profile)
    athena = session.client('athena')
    catalog_name = f"external-cat-{customer.lower()}"
    
    # Get source account ID from AWS config
    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/config'))
    profile_section = f"profile {source_profile}"
    
    if profile_section not in config:
        fail_fast(f"Profile {source_profile} not found in ~/.aws/config")
    
    source_account_id = config[profile_section].get('sso_account_id')
    if not source_account_id:
        fail_fast(f"Could not find sso_account_id for profile {source_profile}")

    def create_catalog():
        athena.create_data_catalog(
            Name=catalog_name,
            Type='GLUE',
            Description=f'Cross-account Glue catalog for {customer.lower()}',
            Parameters={
                'catalog-id': source_account_id,
            }
        )
        logger.info(f"Successfully registered Glue catalog {catalog_name} for account {source_account_id}")

    try:
        create_catalog()
    except athena.exceptions.ClientError as e:
        if 'AlreadyExists' in str(e) or 'has already been created' in str(e):
            logger.info(f"Glue catalog {catalog_name} already exists, attempting to delete and recreate...")
            try:
                athena.delete_data_catalog(Name=catalog_name)
                logger.info(f"Successfully deleted existing catalog {catalog_name}")
                create_catalog()
            except Exception as delete_error:
                fail_fast(f"Failed to delete existing catalog {catalog_name}: {str(delete_error)}")
        else:
            fail_fast(f"Failed to register Glue catalog: {str(e)}")
    except Exception as e:
        fail_fast(f"Failed to register Glue catalog: {str(e)}")

    logger.info(f"\nYou can now query the data using Athena with the following catalog and database:")
    logger.info(f"Catalog: {catalog_name}")
    logger.info(f"Database: {customer.lower()}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update role policy")
    parser.add_argument("-c", "--config", required=True, help="Path to JSON configuration file")
    args = parser.parse_args()
    
    with open(args.config, 'r') as file:
        config = json.load(file)

    cymballic_config = load_cymballic_config()
    global AWS_REGION
    AWS_REGION = cymballic_config.get('aws_region')
    
    source_profile = config.get('aws_profile')
    customer = config.get('customer')
    update_policy(source_profile, customer)
    register_glue_catalog(source_profile, customer)

