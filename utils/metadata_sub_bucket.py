import json
import boto3
from datetime import datetime

import json
import boto3
from datetime import datetime

from config import config

session = boto3.Session(region_name='ap-southeast-2')

s3 = session.client('s3')

BUCKET = config.buckets.metadata
PREFIX = 'metadata'

def put_object(key, data):
    return s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/{key}", Body=data)

def generate_presigned_url(key, expiration=3600, prefer_cache=False):
    presigned_url_sub_bucket = f'{PREFIX}/presigned-urls'
    key_cache_file = f"{presigned_url_sub_bucket}/{key}_{expiration}.txt"
    # Attempt to get the presigned URL from the cache
    if prefer_cache:
        try:
            response = s3.get_object(Bucket=BUCKET, Key=key_cache_file)
            url = response['Body'].read().decode('utf-8')
            # Check that the Expires query parameter is still valid
            if 'Expires' in url:
                expires = int(url.split('Expires=')[1].split('&')[0])
                if expires < datetime.now().timestamp():
                    raise ValueError("Presigned URL expired") 
            else:
                # If the URL is invalid, we will generate a new one
                raise ValueError("Invalid presigned URL format")
            
            return url
        except s3.exceptions.NoSuchKey:
            print(f"Presigned URL not found in cache: {key_cache_file}")
            pass  # The presigned URL is not in the cache, so we will generate a new one
        except Exception as e:
            print(f"Error retrieving presigned URL from cache: {e}")
            pass
    
    response = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': BUCKET, 'Key': f"{PREFIX}/{key}"},
        ExpiresIn=expiration
    )
    s3.put_object(Bucket=BUCKET, Key=f"{key_cache_file}", Body=response)
    return response

def get_object(key, include = None, read_body = True):
    if include is not None and key not in include:
        raise ValueError(f"Key {key} not in include list")
    response = s3.get_object(Bucket=BUCKET, Key=f"{PREFIX}/{key}")
    if not read_body:
        return response
    return response['Body'].read()

def update_object(key, data):
    return s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/{key}", Body=data)

def delete_object(key):
    now = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    copy_source = f"{PREFIX}/{key}"
    key_without_extension, extension = key.rsplit('.', 1)
    recycle_bin_key = f"{PREFIX}/recycle-bin/{key_without_extension}_{now}.{extension}"
    s3.copy_object(
        CopySource={'Bucket': BUCKET, 'Key': copy_source},
        Bucket=BUCKET,
        Key=recycle_bin_key
    )
    return s3.delete_object(Bucket=BUCKET, Key=copy_source)

def list_objects(key='', include_prefix=False):
    prefix = f"{PREFIX}/{key}" if key else PREFIX
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    raw_keys = [item['Key'] for item in response.get('Contents', [])]
    if include_prefix:
        return raw_keys
    return [key.split(f"{PREFIX}/")[1] for key in raw_keys]

def head_object(key):
    response = s3.head_object(Bucket=BUCKET, Key=f"{PREFIX}/{key}")
    return response