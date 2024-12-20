# cymballic

This provides capabilities to:

1. [Optional] Create Parquet files in customer accounts from Postgres RDS
2. Use Parquet files to create Glue tables and databases in customer accounts
3. Query the tables via Athena in Main account (see [cymballic.json](cymballic.json) ).

## Note

This only copies the Glue metadata to the main account. The actual Parquet files on S3 are not copied. 

## Dependencies

### AWS Config

`~/.aws/config` file with SSO capabilities, for example, containing:

```
[profile foo]
sso_start_url = https://d-0000000000.awsapps.com/start
sso_region = us-east-1
sso_account_id = 000000000000
sso_role_name = AdministratorAccess
region = us-east-1
```

### Python dependencies

Run

```
pip3 install -r requirements.txt
```

The profile will be referenced in the connectivity config files.

## Onboarding

1. Create an appropriate JSON file such as [example-config.json](example-config.json).

2. Log in to the appropriate AWS account like `aws sso login --profile <profile-name> [--no-browser]`

3. Run `python3 onboard.py -c example-config.json -t example_table`

4. Run `python3 update.py -s <source-account-id> -d <db-name> -b <bucket-name>`


## Relevant links

 * [Configure cross-account Data Catalog access](https://docs.aws.amazon.com/athena/latest/ug/lf-athena-limitations-cross-account.html)
 * [Configure cross-account access in Athena to Amazon S3 buckets](https://docs.aws.amazon.com/athena/latest/ug/cross-account-permissions.html)
 * [Configure cross-account access to AWS Glue data catalogs](https://docs.aws.amazon.com/athena/latest/ug/security-iam-cross-account-glue-catalog-access.html)
 * [Register a Data Catalog from another account](https://docs.aws.amazon.com/athena/latest/ug/data-sources-glue-cross-account.html)
