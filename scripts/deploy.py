import json
import boto3
import os
import sys
from datetime import datetime

class ConsoleColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    DEFAULT = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# Get stage (deployment folder) from command line argument or use default
# TODO: Consider making "dev" the default deployment folder
if len(sys.argv) < 2:
    stage = "prod"
    print(f"No stage specified, using default: {stage}")
else:
    stage = sys.argv[1]

# Depend on the stage, load from the `.env.dev` or `.env.prod` file
env_file = f'.env.{stage}'
if not os.path.exists(env_file):
    print(f'Error: {env_file} does not exist')
    sys.exit(1)
    
print(f'Loading environment variables from {env_file}')
from dotenv import load_dotenv
load_dotenv(env_file)

# Get configuration from environment variables
# AWS credentials are automatically provided by:
# - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
# - IAM role (when running in AWS Lambda or EC2)
# - Credential files (~/.aws/credentials)
aws_region = os.getenv('AWS_REGION', 'ap-southeast-2')
lambda_function_name = os.getenv('DEPLOYMENT_LAMBDA_FUNCTION_NAME', '')
deployment_bucket = os.getenv('DEPLOYMENT_BUCKET', '')
zip_file = os.getenv('DEPLOYMENT_ZIP_FILE', 'lambda-deployment-package.zip')

session = boto3.Session(region_name=aws_region)

lambda_client = session.client('lambda', region_name=aws_region)
s3_client = session.client('s3', region_name=aws_region)

def is_possibly_dev_environment():
    """Check if the current environment is possibly a development environment.
    
    This can be used to guard against accidental deployments to production.
    """
    KEYWORDS = ['dev', 'development', 'test', 'staging']
    possible_configs = [
        lambda_function_name,
        zip_file,
    ]
    
    for value in possible_configs:
        if any(keyword in value.lower() for keyword in KEYWORDS):
            print(f'{ConsoleColors.WARNING}[WARNING] Possible development environment detected: {value}{ConsoleColors.DEFAULT}')
            return True
    return False

def upload_to_s3(file_path, bucket, key):
    """Upload a file to S3 and return the S3 URL."""
    try:
        print(f'Uploading {file_path} to s3://{bucket}/{key}')
        s3_client.upload_file(file_path, bucket, key)
        print(f'Successfully uploaded to S3')
        return f's3://{bucket}/{key}'
    except Exception as e:
        print(f'Error uploading to S3: {str(e)}')
        raise

def deploy_lambda():
    """Deploy the Lambda function using the specified zip file.
    
    This function uploads the zip file to S3 and updates the Lambda function to use the code from the S3 bucket.
    """
    if stage.lower() == 'prod' and is_possibly_dev_environment():
        print(f"{ConsoleColors.WARNING}[WARNING] You are deploying to production with a configuration that may indicate a development environment.{ConsoleColors.DEFAULT}")
        confirmation = input("Do you want to continue? (yes/no): ").strip().lower()
        if confirmation != 'yes':
            print("Deployment cancelled.")
            return
    
    # Check if zip file exists
    if not os.path.exists(zip_file):
        print(f'Error: {zip_file} does not exist')
        return
    
    # Get file size
    file_size = os.path.getsize(zip_file)
    print(f'Zip file size: {file_size / (1024 * 1024):.2f} MB')
    print(f'Deployment folder: {stage}')
    
    # Generate S3 key with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f'{stage}/{lambda_function_name}_{timestamp}.zip'
    
    try:
        # Upload to S3
        s3_url = upload_to_s3(zip_file, deployment_bucket, s3_key)
        
        # Update Lambda function using S3 URL
        print(f'Updating {lambda_function_name} with S3 package: {s3_url}')
        response = lambda_client.update_function_code(
            FunctionName=lambda_function_name,
            S3Bucket=deployment_bucket,
            S3Key=s3_key,
            Publish=True
        )
        
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f'{lambda_function_name} updated successfully')
            print(f'Function ARN: {response.get("FunctionArn", "N/A")}')
            print(f'Version: {response.get("Version", "N/A")}')
        else:
            print(f'Error updating {lambda_function_name}')
            print(json.dumps(response, indent=2))
            
    except Exception as e:
        print(f'Deployment failed: {str(e)}')

if __name__ == '__main__':
    deploy_lambda()
