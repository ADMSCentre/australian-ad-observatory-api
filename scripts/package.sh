#!/bin/bash

echo "Creating AWS Lambda deployment package with dependencies"

# Get stage from command line argument, default to prod
Stage="${1:-prod}"
echo "Stage: $Stage"

# Validate stage
if [[ "$Stage" != "prod" && "$Stage" != "dev" ]]; then
    echo "Error: Invalid stage '$Stage'. Must be 'prod' or 'dev'."
    exit 1
fi

# Create a temporary directory
TempDir="temp"
ZipFileName="lambda-deployment-package.zip"
mkdir -p $TempDir

# Copy all files to the temporary directory, excluding the following folders
rsync -av --progress . $TempDir --exclude ".git" --exclude ".github" --exclude ".vscode" --exclude "temp" --exclude "__pycache__" --exclude ".venv" --exclude "docs" --exclude "README.md" --exclude "swagger.yaml" --exclude "tests.py" --exclude "scripts" --exclude "$ZipFileName" --exclude "requirements.txt" --exclude "images" --exclude "apitests" --exclude "unittests" --exclude "*.env" --exclude "*.ini"

# Install the dependencies for the 86_64 architecture
echo "Installing dependencies for x86_64 architecture"
pip install -r requirements.txt --platform manylinux2014_x86_64 --target=$TempDir --implementation cp --only-binary=:all: --upgrade --python-version 3.12

# Other dependencies that are not listed, but are required for the Lambda function to run
Dependencies="cffi"
echo "Installing additional dependencies for x86_64 architecture"
pip install $Dependencies --platform manylinux2014_x86_64 --target=$TempDir --implementation cp --only-binary=:all: --upgrade --python-version 3.12

# Delete the existing zip file if it exists
if [ -f $ZipFileName ]; then
    echo "Deleting existing $ZipFileName file"
    rm $ZipFileName
fi

# Zip the contents of the temporary directory (at root)
echo "Zipping the contents of the $TempDir directory"
cd $TempDir
zip -rqdgds 10m ../$ZipFileName *
cd ..

# Delete the temporary directory
rm -rf $TempDir

echo "AWS Lambda deployment package created"