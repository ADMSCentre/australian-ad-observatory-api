import os
import traceback
import urllib
from middlewares import parse_body
from routes import parse_path_parameters, parse_query_parameters, route
import json
from routes import routes
import utils.observations_sub_bucket as observations_sub_bucket
from config import config

@route('reflect', 'POST')
def reflect(event):
    """Reflect the event back to the client.
    
    Return the event object as the response.
    ---
    requestBody:
        required: false
        content:
            application/json:
                schema:
                    type: object
    responses:
        200:
            description: A successful reflection
            content:
                application/json:
                    schema:
                        type: object
        400:
            description: A failed reflection
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            comment:
                                type: string
    """
    return event

@route('/hello', 'GET')
def hello():
    api_name = config.deployment.lambda_function_name
    return {'message': f"Hello from {api_name}!"}

@route('/hello/{user_id}', 'GET')
def hello(event):
    user_id = event['pathParameters']['user_id']
    return {'message': f'Hello, {user_id}!'}

def handle_api_gateway_event(event_raw, context):
    try:
        event, response, context = parse_body(event_raw, context, None)
        event['headers'] = {k.lower(): v for k, v in event.get('headers', {}).items()}
        path = event['path']
        method = event['httpMethod']
        if not path.startswith('/'):
            path = f'/{path}'
        if path.endswith('/'):
            path = path[:-1]
        
        path, query_params = parse_query_parameters(path)    
        route, path_params = parse_path_parameters(path)
        # print(f'Route: {route}')
        # print(f'Path params: {path_params}')
        # print(f'Query params: {query_params}')
        # print(f'Method: {method}')
        if query_params is not None and len(query_params) > 0:
            event['queryStringParameters'] = query_params
        if path_params is not None and len(path_params) > 0:
            event['pathParameters'] = path_params
        
        
        if route in routes and method in routes[route]:
            action = routes[route][method]
            _, response, _ = action(event, response, context)
            return response.body

        return {
            'statusCode': 404,
            'isBase64Encoded': False,
            'headers': {
                'Content-Type': 'application/json',
            },
            'body': json.dumps({
                'success': False,
                'comment': 'ACTION_NOT_FOUND',
                'error': f'No route found for "{path}" with method "{method}"'
            })
        }
    except Exception as e:
        print(traceback.format_exc())
        return {
            'statusCode': 500,
            'isBase64Encoded': False,
            'headers': {
                'Content-Type': 'application/json',
            },
            'body': json.dumps({
                'success': False,
                'comment': 'INTERNAL_SERVER_ERROR',
                'error': str(e)
            })
        }

def handle_s3_event(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        # Only allow bucket to be the observations bucket
        if bucket != observations_sub_bucket.MOBILE_OBSERVATIONS_BUCKET:
            raise Exception(f'This lambda function does not support bucket {bucket}')
        
        # If the key has the following format:
        # <observer_id>/clip_classifications/<observation_id>.json
        # Then it is a clip classification file, process it
        if '/clip_classifications/' in key and key.endswith('.json'):
            print(f'Processing clip classification object {key}')
            parts = key.split('/')
            parts = [part for part in parts if part != '']  # Remove empty parts
            if len(parts) != 3:
                raise Exception(f'Invalid clip classification key format: {key}')
            observer_id = parts[0]
            observation_id = parts[2].replace('.json', '')
            
            # Import and process the clip classification
            from utils.etl.clip_classification import process_single_ad
            success = process_single_ad(observer_id, '', observation_id)
            
            return {
                'success': success,
                'observer_id': observer_id,
                'observation_id': observation_id,
                'message': 'Clip classification processed' if success else 'Failed to process clip classification'
            }
        
        # If the key has the following format:
        # <observer_id>/rdo/<timestamp>.<observation_id>/output.json
        # Then it is an RDO so request it to be indexed via the API
        if key.endswith('/output.json'):
            print(f'Processing RDO object {key}')
            parts = key.split('/')
            parts = [part for part in parts if part != ''] # Remove empty parts
            if len(parts) != 4:
                raise Exception(f'Invalid key format: {key}')
            observer_id = parts[0]
            timestamp_observation_id = parts[2].split('.')
            timestamp = timestamp_observation_id[0]
            observation_id = timestamp_observation_id[1]
            return invoke({
                "path": f'ads/{observer_id}/{timestamp}.{observation_id}/request_index',
                "httpMethod": 'GET',
                "headers": {
                    'Content-Type': 'application/json',
                }
            })
        
        return key
    except Exception as e:
        print(f'Error getting object {key} from bucket {bucket}')
        print(e)
        raise e

def lambda_handler(event, context):
    # If the event has records, it is an S3 event, so handle S3 event
    if event.get("Records"):
        print('Handling S3 event', event)
        return handle_s3_event(event, context)
    # If the event has a path, it is an API Gateway event, so handle API call
    if event.get("path"):
        print('Handling API Gateway event', event) 
        return handle_api_gateway_event(event, context)

def invoke(event, verbose=False):
    result = lambda_handler({
        **event,
        "headers": {
            'Content-Type': 'application/json',
            **(event.get('headers', {}))
        }
    }, {})
    if verbose: print(json.dumps(result, indent=2))
    return result

if __name__ == "__main__":
    event = {
        "path": 'ads/153ccc28-f378-4274-98d3-0258574a03c5/1732759316233.5933a2d9-0e55-41b8-99a7-1a308a231956/request_index', 
        "httpMethod": 'GET',
        "headers": {
            'Content-Type': 'application/json',
        }
    }
    invoke(event, verbose=True)