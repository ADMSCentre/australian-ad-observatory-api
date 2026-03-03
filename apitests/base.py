# Base file for testing the API endpoints.
import json
from typing import Literal
from lambda_function import lambda_handler as local_handler
from config import config
from utils.hash_password import hash_password
# Need to access the database directly as without functional authentication, it is not possible to create users through the API (which needs authentication).
from db.shared_repositories import users_repository, user_identities_repository
import time

username = config.test.username
password = config.test.password



def create_test_user():
    """
    Create a test user for testing purposes.
    
    This function creates a test user with the specified username and password, and assigns it an admin role.
    """
    # Create user in users table
    user_data = {
        'full_name': 'Test User (Auto-generated)',
        'enabled': True,
        'role': 'admin'
    }
    with users_repository.create_session() as user_session:
        user_entity = user_session.create(user_data)
        user_id = user_entity.id
    
    # Create identity in user_identities table
    with user_identities_repository.create_session() as identity_session:
        identity_data = {
            'user_id': user_id,
            'provider': 'local',
            'provider_user_id': username,
            'password': hash_password(password),
            'created_at': int(time.time())
        }
        identity_session.create(identity_data)

def ensure_test_user_exists():
    """
    Ensure that a test user exists in the system.
    
    This function checks if a test user with the specified username exists. If not, it creates one.
    """
    try:
        # Attempt to get the user identity
        with user_identities_repository.create_session() as identity_session:
            user_identity = identity_session.get_first({
                'provider': 'local',
                'provider_user_id': username
            })
            if user_identity is None:
                create_test_user()
    except Exception as e:
        print(f"Error ensuring test user exists: {e}")

def get_login_token():
    ensure_test_user_exists()
    data = {
        'username': username,
        'password': password
    }
    event = {
        'path': '/auth/login',
        'httpMethod': 'POST',
        'body': json.dumps(data)
    }
    response = local_handler(event, None)
    return json.loads(response['body'])['token']

def live_handler(event, context):
    # Invoke the live lambda function handler (for testing against the deployed API)
    import boto3
    session = boto3.Session(region_name=config.aws.region)
    lambda_client = session.client('lambda')
    response = lambda_client.invoke(
        FunctionName=config.deployment.lambda_function_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(event)
    )
    response_payload = response['Payload'].read()
    return json.loads(response_payload)

def execute_endpoint(endpoint:str, 
                     method: Literal['GET', 'POST', 'DELETE', 'PUT', 'PATCH']='GET', 
                     body:dict | None = None, 
                     headers:dict | None = None, 
                     auth = False,
                     use_live = False
                    ):
    """
    Execute a local API endpoint with the given method and body.
    
    :param endpoint: The API endpoint to call.
    :param method: The HTTP method to use (default is 'GET').
    :param body: The request body (default is None).
    :param headers: Additional headers to include in the request (default is None).
    :param auth: Whether to include an Authorization header with a login token (default is False).
    :param use_live: Whether to call the live deployed API instead of the local handler (default is False).
    :return: The response from the API call.
    """
    if auth:
        token = get_login_token()
        headers = headers or {}
        headers['Authorization'] = f'Bearer {token}'
    
    event = {
        'httpMethod': method,
        'path': endpoint,
        'headers': headers or {},
    }
    
    if body is not None:
        event['body'] = json.dumps(body)
    
    response = local_handler(event, None) if not use_live else live_handler(event, None)
    if 'body' in response and response['body'] is not None:
        response['body'] = json.loads(response['body'])
    return response