from config import config
from dataclasses import dataclass, field
from opensearchpy import OpenSearch, RequestsHttpConnection
from utils import observations_sub_bucket
from requests_aws4auth import AWS4Auth
import boto3

from utils.indexer.registry import IndexRegistry
from utils.reduce_rdo.reduce_rdo import RdoReducer

OPEN_SEARCH_ENDPOINT = config.open_search.endpoint
SERVICE = 'aoss'
AWS_CONFIG = config.aws

session = boto3.Session(
    region_name=AWS_CONFIG.region,
    aws_access_key_id=AWS_CONFIG.access_key_id,
    aws_secret_access_key=AWS_CONFIG.secret_access_key
)
credentials = session.get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                   AWS_CONFIG.region, SERVICE, session_token=credentials.token)

client = OpenSearch(
    hosts=[OPEN_SEARCH_ENDPOINT],
    http_auth=awsauth,
    connection_class=RequestsHttpConnection
)

@dataclass
class AdWithRDO:
    observer_id: str
    timestamp: str
    ad_id: str
    
    rdo_path: str = field(init=False)
    open_search_id: str = field(init=False)
    
    def __post_init__(self):
        self.rdo_path = f"{self.observer_id}/rdo/{self.timestamp}.{self.ad_id}/output.json"
        self.open_search_id = f"{self.observer_id}.{self.ad_id}"
    
    def fetch_rdo(self, remove_fields=False):
        if not hasattr(self, "rdo_content"):
            self.rdo_content = observations_sub_bucket.read_json_file(self.rdo_path)
        
        if not remove_fields:
            return self.rdo_content
        
        fields_to_remove = [
            ['enrichment', 'meta_adlibrary_scrape', 'comparisons'],
            ['enrichment', 'ccl'],
            ['enrichment', 'media'],
            ['media'],
            ['observation', 'whitespace_derived_signature']
        ]

        for field in fields_to_remove:
            try:
                current_level = self.rdo_content
                for key in field[:-1]:
                    current_level = current_level.get(key, {})
                del current_level[field[-1]]
            except KeyError:
                pass
            
        return self.rdo_content

try:
    latest_index = IndexRegistry().get_latest(status='ready')
    LATEST_READY_INDEX = latest_index.name if latest_index else None
except Exception as e:
    print(f"Error getting latest ready index: {e}")
    LATEST_READY_INDEX = None

class RdoOpenSearch:
    def __init__(self, index: str | None = LATEST_READY_INDEX):
        self.index = index
        self.client = client
    
    def create_pit(self):
        # Create a Point In Time (PIT) for the index
        response = self.client.create_pit(index=self.index, params={
            "keep_alive": "5m"
        })
        print(f"Created PIT: {response}")
        return response['pit_id']

    
    def create_index(self):
        # Create the index with the specified mapping if it does not exist
        try:
            self.client.indices.create(index=self.index)
        except Exception as e:
            print(f"Index creation failed: {e}")
    
    def delete_index(self):
        # Delete the index if it exists
        try:
            self.client.indices.delete(index=self.index)
        except Exception as e:
            print(f"Index deletion failed: {e}")
    
    def search(self, query):
        return self.client.search(index=self.index, body=query, params={
            "timeout": 300
        })
    
    def get(self, id):
        return self.client.get(index=self.index, id=id, params={
            "timeout": 300
        })
    
    def put(self, ad_with_rdo: AdWithRDO, reduce=True):
        # Attempt to reduce the RDO content if a reducer is provided
        body = ad_with_rdo.fetch_rdo()
        
        # If the body contains an `is_user_disabled` field, delete the document instead of
        # indexing it
        if isinstance(body, dict) and body.get('is_user_disabled'):
            return self.delete(ad_with_rdo.open_search_id)
        
        if reduce:
            version = 1
            # Attempt to get the version in the body
            if isinstance(body, dict) and 'version' in body:
                version = body['version']
            reducer = RdoReducer().set_version(version)
            body = reducer(body)
        return self.client.index(index=self.index, id=ad_with_rdo.open_search_id, body=body, params={
            "timeout": 300
        })
    
    def delete(self, id):
        return self.client.delete(index=self.index, id=id, params={
            "timeout": 300
        })

def get_hit_source_id(hit):
    # Extract the observer id and observation id
    source = hit.get("_source")
    observer = source.get("observer")
    observation = source.get("observation")
    return f"{observer['uuid']}/temp/{observation['uuid']}"