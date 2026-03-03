import json
import boto3
from botocore.exceptions import ClientError
from db.clients.base_storage_client import BaseStorageClient

from config import config

def create_file_name_from_keys(keys: dict, order=list[str]) -> str:
    """Create a file name from the keys dictionary.
    
    Args:
        keys (dict): A dictionary of keys to create the file name from.
    Returns:
        str: A string representing the file name.
    """
    # Ensure the keys are in the specified order
    ordered_keys = {key: keys[key] for key in order if key in keys}
    # Filter out None values and join the keys with '__'
    return '__'.join(f"{v}" for v in ordered_keys.values() if v is not None)

def create_keys_from_file_name(filename: str, keys_order: list[str]) -> dict:
    """Create a dictionary of keys from the file name.
    
    Args:
        filename (str): The file name to create the keys from.
        keys (list[str]): A list of keys to use for creating the dictionary.
    Returns:
        dict: A dictionary with keys from the provided list and values extracted from the file name.
    """
    parts = filename.split('__')
    return {key: parts[i] for i, key in enumerate(keys_order) if i < len(parts) and parts[i] != 'None'}

class S3StorageClient(BaseStorageClient):
    """A client for Amazon S3 storage."""
    def __init__(self, **config: dict):
        """Initialize the S3 storage client with configuration parameters.

        Args:
            bucket (str): The name of the S3 bucket to use.
            prefix (str): The subdirectory in the S3 bucket where objects are stored.
            extension (str): The file extension for the objects stored in S3.
            keys (list[str]): A list of primary keys to identify objects in the storage.
        """
        super().__init__(**config)
        self.s3 = None
        self.bucket = config.get('bucket')
        if not self.bucket:
            raise ValueError("Bucket name must be provided in the configuration.")
        # The subdirectory in the S3 bucket where objects are stored,
        # should not end with a slash
        self.prefix = config.get('prefix', '')
        if self.prefix.endswith('/'):
            self.prefix = self.prefix[:-1]
        # The file extension for the objects stored in S3
        self.extension = config.get('extension', 'json')
        self.keys = config.get('keys', ['id'])

    def connect(self):
        """Connect to the S3 service."""
        session = boto3.Session(region_name='ap-southeast-2')
        self.s3 = session.client('s3')
        self.connected = True

    def disconnect(self):
        """Disconnect from the S3 service."""
        self.s3 = None
        self.connected = False

    def get(self, keys):
        """Retrieve an object from the S3 bucket."""
        try:
            filename = create_file_name_from_keys(keys, order=self.keys)
            response = self.s3.get_object(Bucket=self.bucket, Key=f"{self.prefix}/{filename}.{self.extension}")
            body = response['Body'].read().decode('utf-8')
            return [json.loads(body)]
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise

    def put(self, value):
        """Store an object in the S3 bucket."""
        filename = create_file_name_from_keys(value, order=self.keys)
        self.s3.put_object(Bucket=self.bucket, Key=f"{self.prefix}/{filename}.{self.extension}", Body=json.dumps(value))

    def delete(self, keys: dict):
        """Delete an object from the S3 bucket."""
        filename = create_file_name_from_keys(keys, order=self.keys)
        self.s3.delete_object(Bucket=self.bucket, Key=f"{self.prefix}/{filename}.{self.extension}")

    def list_ids(self):
        """List all object IDs in the S3 bucket."""
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
            return [
                create_keys_from_file_name(obj['Key'].split('/')[-1].replace(f'.{self.extension}', ''), self.keys)
                for obj in response.get('Contents', [])
            ]
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                return []
            raise

    def list(self):
        """List all objects in the S3 bucket."""
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
            return [
                {
                    'keys': create_keys_from_file_name(obj['Key'].split('/')[-1].replace(f'.{self.extension}', ''), self.keys),
                    'value': json.loads(self.s3.get_object(Bucket=self.bucket, Key=obj['Key'])['Body'].read().decode('utf-8'))
                }
                for obj in response.get('Contents', [])
            ]
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                return []
            raise