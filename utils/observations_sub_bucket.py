import boto3
import os
import json

from config import config

session_sydney = boto3.Session(region_name='ap-southeast-2')

MOBILE_OBSERVATIONS_BUCKET = config.buckets.observations

client = session_sydney.client('s3')

# Maximum number of pagination iterations to prevent infinite loops
MAX_LIST_DIR_ITERS = 1000

def list_dir(path="", list_all=False):
    # If the path does not have an extension, it is a directory
    if not os.path.splitext(path)[1] and not path.endswith('/'):
        path += '/'
    kwargs = {
        'Bucket': MOBILE_OBSERVATIONS_BUCKET, 
        'Delimiter': '/'
    }
    
    if path and path not in ["/", "", ".", "./"]:
        kwargs['Prefix'] = path
    
    dir_elements = []
    
    def process_response(response):
        # Process directories
        for content in response.get('CommonPrefixes', []):
            dir_name = content.get('Prefix')
            dir_elements.append(dir_name)
        
        # Process files
        for content in response.get('Contents', []):
            file_name = content.get('Key')
            dir_elements.append(file_name)
    
    # Single request, up to 1000 objects
    if not list_all:
        client_response = client.list_objects_v2(**kwargs)
        process_response(client_response)
        return dir_elements
    
    # Otherwise, use pagination to get all objects when list_all=True
    continuation_token = None
    iteration_count = 0
    while iteration_count < MAX_LIST_DIR_ITERS:
        list_kwargs = dict(MaxKeys=1000, **kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        
        client_response = client.list_objects_v2(**list_kwargs)
        process_response(client_response)
        
        # Check if there are more results to fetch
        if not client_response.get('IsTruncated'):
            break
        continuation_token = client_response.get('NextContinuationToken')
        iteration_count += 1
    
    # Log a warning if we hit the maximum iterations
    if iteration_count >= MAX_LIST_DIR_ITERS:
        print(f"Warning: Hit maximum iterations ({MAX_LIST_DIR_ITERS}) while listing S3 objects. Results may be incomplete.")
    
    return dir_elements

def read_json_file(path):
    try:
        obj = client.get_object(Bucket=MOBILE_OBSERVATIONS_BUCKET, Key=path)
        data = obj['Body'].read().decode('utf-8')
        return json.loads(data)
    except Exception as e:
        print('Error reading file:', path)
        print(e)
        return None
    
def try_get_object(key, bucket=MOBILE_OBSERVATIONS_BUCKET):
    try:
        return client.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        return None

class Observer:
    def __init__(self, observer_id):
        self.observer_id = observer_id
        # self.expected_subdirs = [
        #     'display',
        #     'meta_adlibrary_scrape',
        #     'misc',
        #     'rdo',
        #     'relation',
        #     'stitching',
        #     'temp'
        # ]
        
    def read_json_file(self, path):
        return read_json_file(f"{self.observer_id}/{path}")
    
    def get_output_from_scrape(self, timestamp, ad_id):
        prefix = f"{self.observer_id}/meta_adlibrary_scrape/{timestamp}.{ad_id}"
        filename = "output_from_scrape.json"
        return read_json_file(f"{prefix}/{filename}")
        
    def get_output_from_restitcher(self, timestamp, ad_id):
        prefix = f"{self.observer_id}/stitching/{timestamp}.{ad_id}"
        filename = "output_from_restitcher.json"
        return read_json_file(f"{prefix}/{filename}")
    
    def get_relation_outputs(self, timestamp, ad_id):
        prefix = f"{self.observer_id}/relation/{timestamp}.{ad_id}"
        modes = {
            "ocr": "output.json",
            "images": "images_comparison.json",
            "videos": "videos_comparison.json"
        }
        outputs = {}
        for mode, filename in modes.items():
            outputs[mode] = read_json_file(f"{prefix}/{filename}")
        return outputs

    def get_ad_content(self, timestamp, ad_id):
        prefix = f"{self.observer_id}/temp/{timestamp}.{ad_id}"
        filename = "adContent.json"
        return read_json_file(f"{prefix}/{filename}")

    def get_pre_constructed_rdo(self, timestamp, ad_id):
        try:
            prefix = f"{self.observer_id}/rdo/{timestamp}.{ad_id}"
            filename = "output.json"
            return read_json_file(f"{prefix}/{filename}")
        except Exception as e:
            prefix = f"{self.observer_id}/rdo/{ad_id}"
            filename = "output.json"
            return read_json_file(f"{prefix}/{filename}")
        
    def get_latest_csr_presign_url(self):
        # List the content of the observer_id/csr directory in the S3 bucket
        csr_dir = f"{self.observer_id}/csr/"
        csr_files = list_dir(csr_dir)
        
        # If there are no CSR files, return None
        if not csr_files:
            return None
        
        # Sort the files by modification time and use the latest one
        csr_files.sort(key=lambda x: x.split('/')[-1], reverse=True)
        latest_csr_file = csr_files[0]
        
        # Generate a presigned URL for the latest CSR file
        presigned_url = client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': MOBILE_OBSERVATIONS_BUCKET,
                'Key': latest_csr_file
            },
            ExpiresIn=3600
        )
        return presigned_url

if __name__ == "__main__":
    # observer_id = "f60b6e94-7625-4044-9153-1f70863f81d8"
    # timestamp = "1729569092202"
    # ad_id = "d14f1dc0-5685-49e5-83d9-c46e29139373"
    # observer = Observer(observer_id)
    pass
    