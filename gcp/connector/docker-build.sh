#!/bin/bash

set -x

aws s3 --profile main-profile cp s3://metadata/lambdas/athena-gcs-2022.47.1.jar .

docker build -t athena-gcs-connector .

# Create ECR repository (if it doesn't exist)
aws ecr --profile main-profile create-repository --repository-name athena-gcs-connector

# TODO get from cymballic.json
export AWS_ACCOUNT_ID=058264346350

# Get ECR login token
aws ecr  --profile main-profile  get-login-password --region us-east-1 | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.us-east-1.amazonaws.com

# Tag the image
docker tag athena-gcs-connector:latest ${AWS_ACCOUNT_ID}.dkr.ecr.us-east-1.amazonaws.com/athena-gcs-connector:latest

# Push the image
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.us-east-1.amazonaws.com/athena-gcs-connector:latest
