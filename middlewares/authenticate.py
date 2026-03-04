import boto3
from config import config
from utils import jwt
from utils.api_key import get_api_key_entity, update_last_used
from db.shared_repositories import users_repository

session_us_east = boto3.Session(region_name='us-east-2')

def is_user_exists(user_id: str) -> bool:
    with users_repository.create_session() as session:
        user = session.get_first({
            'id': user_id
        })
        return user is not None


def authenticate_with_jwt(event, response, context):
    """
    Authenticate using JWT token from Authorization: Bearer header.
    
    Returns:
        Tuple of (event, response, context) with user/identity set,
        or (event, response, context) with 401 status on failure
    """
    headers = event.get('headers', {})
    bearer = headers.get('authorization', None)
    
    if bearer is None or not bearer.startswith('Bearer '):
        return None  # No JWT present, not an error
    
    token = bearer[7:]
    if not jwt.verify_token(token):
        response.status(401).json({
            "success": False,
            "comment": "SESSION_TOKEN_EXPIRED",
        })
        return event, response, context
    
    json_web_token = jwt.JsonWebToken.from_token(token)
    
    # If the claim is a guest, skip the user existence check
    if json_web_token.role == 'guest':
        event['identity'] = json_web_token.identity
        event['user'] = json_web_token.user
        event['auth_method'] = 'jwt'
        print(f"[Authentication] Guest user successfully verified via JWT: {event['user']} with identity {event['identity']}")
        return event, response, context
    
    # Ensure the user exists in the database
    if not is_user_exists(json_web_token.user.id):
        response.status(401).json({
            "success": False,
            "comment": "USER_NOT_FOUND",
        })
        return event, response, context
    
    event['identity'] = json_web_token.identity
    event['user'] = json_web_token.user
    event['auth_method'] = 'jwt'
    print(f"[Authentication] User successfully verified via JWT: {event['user']} with identity {event['identity']}")
    return event, response, context


def authenticate_with_api_key(event, response, context):
    """
    Authenticate using API key from X-API-Key header.
    
    Returns:
        Tuple of (event, response, context) with user set and identity=None,
        or (event, response, context) with 401 status on failure
    """
    headers = event.get('headers', {})
    api_key = headers.get('x-api-key', None)
    
    if api_key is None:
        return None  # No API key present, not an error
    
    # Verify API key and get associated user
    key, user = get_api_key_entity(api_key)
    
    if user is None:
        response.status(401).json({
            "success": False,
            "comment": "INVALID_API_KEY",
        })
        return event, response, context
    
    # Check if user is enabled
    if not user.enabled:
        response.status(401).json({
            "success": False,
            "comment": "USER_DISABLED",
        })
        return event, response, context
    
    event['identity'] = None  # API keys don't have an associated identity
    event['user'] = user
    event['auth_method'] = 'api_key'
    update_last_used(key.id)
    print(f"[Authentication] User successfully verified via API key: {user.id}")
    return event, response, context


def authenticate(event, response, context):
    """Middleware to authenticate the user using JWT or API key.
    
    This function supports two authentication methods:
    1. JWT via Authorization: Bearer <token> header
    2. API key via X-API-Key: <key> header
    
    Priority: JWT is checked first. If both are present, JWT is used.
    
    The authenticated user is passed to the applied function via event['user'].
    For JWT authentication, event['identity'] contains the identity provider info.
    For API key authentication, event['identity'] is None.
    The authentication method used is stored in event['auth_method'] ('jwt' or 'api_key').
    
    Appends the following OpenAPI documentation to the applied function:
    
    ---
    security:
        - bearerAuth: []
        - apiKeyAuth: []
    """
    headers = event.get('headers', None)
    if headers is None:
        response.status(401).json({
            "success": False,
            "comment": f'NO_HEADERS',
        })
        return event, response, context
    
    # Try JWT authentication first (maintains backward compatibility)
    jwt_result = authenticate_with_jwt(event, response, context)
    if jwt_result is not None:
        return event, response, context
    
    # Try API key authentication
    api_key_result = authenticate_with_api_key(event, response, context)
    if api_key_result is not None:
        return event, response, context
    
    # No valid authentication method provided
    response.status(401).json({
        "success": False,
        "comment": "NO_VALID_AUTHENTICATION",
    })
    return event, response, context