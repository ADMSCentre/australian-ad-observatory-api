"""Lambda handler for S3 event processing.

This module handles S3 events for the ingestion microservice:
- Clip classification files: Process and store in RDS
- RDO output files: Index to RDS and OpenSearch
"""

import traceback
import urllib.parse

import utils.observations_sub_bucket as observations_sub_bucket
from utils.etl.clip_classification import process_single_ad as process_clip_classification
from utils.indexer.indexer import Indexer
from utils.indexer.registry import IndexRegistry


def handle_clip_classification(key: str) -> dict:
    """Process a clip classification file from S3.
    
    Expected key format: {observer_id}/clip_classifications/{observation_id}.json
    
    Args:
        key: The S3 object key
        
    Returns:
        Result dictionary with success status and details
    """
    print(f'Processing clip classification object {key}')
    parts = key.split('/')
    parts = [part for part in parts if part != '']  # Remove empty parts
    
    if len(parts) != 3:
        raise ValueError(f'Invalid clip classification key format: {key}')
    
    observer_id = parts[0]
    observation_id = parts[2].replace('.json', '')
    
    success = process_clip_classification(observer_id, '', observation_id)
    
    return {
        'success': success,
        'type': 'clip_classification',
        'observer_id': observer_id,
        'observation_id': observation_id,
        'message': 'Clip classification processed' if success else 'Failed to process clip classification'
    }


def handle_rdo_output(key: str) -> dict:
    """Process an RDO output file from S3 and index it.
    
    Expected key format: {observer_id}/rdo/{timestamp}.{observation_id}/output.json
    
    Args:
        key: The S3 object key
        
    Returns:
        Result dictionary with success status and details
    """
    print(f'Processing RDO object {key}')
    parts = key.split('/')
    parts = [part for part in parts if part != '']  # Remove empty parts
    
    if len(parts) != 4:
        raise ValueError(f'Invalid RDO key format: {key}')
    
    observer_id = parts[0]
    timestamp_observation_id = parts[2].split('.')
    timestamp = timestamp_observation_id[0]
    observation_id = timestamp_observation_id[1]
    
    # Get the latest ready index for OpenSearch
    try:
        registry = IndexRegistry()
        latest_index = registry.get_latest(status='ready')
        index_name = latest_index.name if latest_index else None
    except Exception as e:
        print(f"Warning: Could not get latest index, skipping OpenSearch indexing: {e}")
        index_name = None
    
    # Index to RDS (always) and OpenSearch (if index available)
    indexer = Indexer(skip_on_error=True, index_name=index_name)
    
    # Always index to RDS
    indexer.put_index_rds(observer_id, timestamp, observation_id)
    
    # Index to OpenSearch if we have an index
    if index_name:
        indexer.put_index_open_search(observer_id, timestamp, observation_id)
    
    return {
        'success': True,
        'type': 'rdo_output',
        'observer_id': observer_id,
        'timestamp': timestamp,
        'observation_id': observation_id,
        'index_name': index_name,
        'message': 'RDO indexed successfully'
    }


def handle_s3_event(event: dict, context) -> dict:
    """Handle an S3 event notification.
    
    Dispatches to appropriate handler based on the object key pattern:
    - /clip_classifications/*.json -> handle_clip_classification
    - /rdo/*/output.json -> handle_rdo_output
    
    Args:
        event: The S3 event from Lambda
        context: The Lambda context
        
    Returns:
        Result dictionary from the appropriate handler
    """
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], 
        encoding='utf-8'
    )
    
    # Validate bucket
    if bucket != observations_sub_bucket.MOBILE_OBSERVATIONS_BUCKET:
        raise ValueError(f'Unsupported bucket: {bucket}')
    
    # Route to appropriate handler based on key pattern
    if '/clip_classifications/' in key and key.endswith('.json'):
        return handle_clip_classification(key)
    
    if key.endswith('/output.json') and '/rdo/' in key:
        return handle_rdo_output(key)
    
    # Unknown file type - log and return
    print(f'Ignoring unrecognized S3 object: {key}')
    return {
        'success': True,
        'type': 'ignored',
        'key': key,
        'message': 'Object key pattern not recognized, skipping'
    }


def lambda_handler(event: dict, context) -> dict:
    """Main Lambda entry point.
    
    This function handles S3 event notifications only.
    
    Args:
        event: The Lambda event (must be an S3 event)
        context: The Lambda context
        
    Returns:
        Result dictionary with processing status
    """
    try:
        # S3 events have a Records array
        if not event.get('Records'):
            return {
                'success': False,
                'error': 'Not an S3 event - missing Records field'
            }
        
        print('Handling S3 event:', event)
        return handle_s3_event(event, context)
        
    except Exception as e:
        print(f'Error processing event: {e}')
        print(traceback.format_exc())
        return {
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }


# For local testing
if __name__ == "__main__":
    # Example: Test with a mock S3 event for an RDO output
    test_event = {
        "Records": [{
            "s3": {
                "bucket": {"name": observations_sub_bucket.MOBILE_OBSERVATIONS_BUCKET},
                "object": {
                    "key": "153ccc28-f378-4274-98d3-0258574a03c5/rdo/1732759316233.5933a2d9-0e55-41b8-99a7-1a308a231956/output.json"
                }
            }
        }]
    }
    
    result = lambda_handler(test_event, {})
    print(f"Result: {result}")
