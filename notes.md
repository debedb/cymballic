Glue Crawler Approach for Schema Inference

Alternative approach to schema inference using AWS Glue Crawler:

1. Instead of trying to read schema directly from parquet file:
   - Create table with empty columns first
   - Create and run a Glue crawler to infer schema
   - Crawler will automatically update the table with correct schema

2. Implementation notes:
   - Create crawler with:
     - Name: {database_name}-{table_name}-crawler
     - Role: AWSGlueServiceRole-default
     - Database: {database_name}
     - S3 Target: s3://{bucket_name}/{table_name}/
   - Start crawler and wait for completion
   - Crawler will automatically update table schema

3. Considerations:
   - Requires additional IAM permissions for Glue service role
   - Takes longer to complete (crawler runtime)
   - More reliable for cross-account setups
   - No need for direct S3 read access
   - Handles complex parquet schemas better

4. Required IAM permissions for cross-account:
   - Source account needs:
     - glue:CreateCrawler
     - glue:StartCrawler
     - glue:GetCrawler
   - Target account needs:
     - glue:GetTable
     - glue:UpdateTable

5. Trade-offs:
   + More reliable schema inference
   + Works without direct S3 access
   + Handles nested structures
   - Slower execution
   - Additional IAM setup
   - Higher AWS cost (crawler runtime)