#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <stage: dev | prod>"
    exit 1
fi

STAGE=$1
ENV_FILE=".env.$STAGE"

# Validate stage input
if [[ "$STAGE" != "dev" && "$STAGE" != "prod" ]]; then
    echo "Error: Stage must be 'dev' or 'prod'."
    exit 1
fi

if [ "$STAGE" == "dev" ]; then
    FUNCTION_NAME="fta-mobile-observations-api-dev"
elif [ "$STAGE" == "prod" ]; then
    FUNCTION_NAME="fta-mobile-observations-api"
fi

# Check if the corresponding .env file exists
if [ ! -f "$ENV_FILE" ]; then
    echo "Error: Configuration file $ENV_FILE not found."
    exit 1
fi

echo "Updating $FUNCTION_NAME for stage: $STAGE using $ENV_FILE..."

# Parse .env file into CSV format (KEY1=VAL1,KEY2=VAL2)
# Excludes 
# - comments and empty lines
# - lines starting with AWS_ as they are already set in the Lambda environment and should not be overridden
# - lines starting with TEST_ as they are only used for testing
VARIABLES=$(grep -v '^#' "$ENV_FILE" | grep -v '^$' | grep -v '^AWS_' | grep -v '^TEST_' | xargs | sed 's/ /,/g')

# Execute AWS CLI command
aws lambda update-function-configuration \
    --function-name "$FUNCTION_NAME" \
    --environment "Variables={$VARIABLES}" \
    --region "ap-southeast-2" \
    --profile "cli" \
    --output "json"

if [ $? -eq 0 ]; then
    echo "Successfully updated environment variables for $FUNCTION_NAME."
else
    echo "Failed to update environment variables."
    exit 1
fi