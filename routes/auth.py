from models.user import User, UserIdentity
from routes import route
from middlewares.authenticate import authenticate
from utils import Response, use, jwt
from config import config
from utils.auth_providers import client as cilogon_client
from utils.security import sign_state_data, verify_signed_state_data
from db.shared_repositories import users_repository, user_identities_repository
import time

FRONTEND_URL = config.app.frontend_url
REDIRECT_URI = config.cilogon.redirect_uri


@route("auth/login", "POST")
def login(event, response: Response):
    """Log the user in and return a JSON web token.

    Log the user in and create a JSON web token for the user, which can be used to authenticate the user in future requests.
    ---
    tags:
        - auth
    requestBody:
        required: true
        content:
            application/json:
                schema:
                    type: object
                    properties:
                        username:
                            type: string
                        password:
                            type: string
    responses:
        200:
            description: A successful login
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            token:
                                type: string
        400:
            description: A failed login
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
    """
    username = event["body"].get("username")
    if not username:
        return response.status(400).json({"success": False, "comment": "Username is required"})
    password = event["body"].get("password")
    if not password:
        return response.status(400).json({"success": False, "comment": "Password is required"})
    try:
        return {"success": True, "token": jwt.create_session_token(username, password)}
    except Exception as e:
        return response.status(400).json({"success": False, "comment": str(e)})


@route("auth/verify", "POST")
def verify(event, response: Response):
    """Verify the JSON web token.

    Return whether the JSON web token is valid.
    ---
    tags:
        - auth
    requestBody:
        required: true
        content:
            application/json:
                schema:
                    type: object
                    properties:
                        token:
                            type: string
    responses:
        200:
            description: A successful verification
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
        400:
            description: A failed verification
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                                example: False
                            comment:
                                type: string
                                example: 'VERIFY_FAILED'
    """
    token = event["body"].get("token")
    if not token:
        return response.status(400).json({"success": False, "comment": "Token is required"})
    if jwt.verify_token(token):
        return {"success": True}
    return response.status(400).json({"success": False, "comment": "VERIFY_FAILED"})

@route("auth/cilogon/login", "GET")
def cilogon_login(event, response: Response):
    """Initiates CILogon OIDC login flow.
    ---
    tags:
      - auth
      - cilogon
    description: Redirects the user to the CILogon authorization endpoint to start the OpenID Connect authentication flow. Sets a temporary state cookie for CSRF protection.
    parameters: []
    responses:
      302:
        description: Successful initiation of the login flow. Redirects the user's browser to CILogon.
        headers:
          Location:
            description: The URL of the CILogon authorization endpoint where the user should be redirected.
            schema:
              type: string
              format: uri
          Set-Cookie:
            description: Sets the `cilogon_oauth_state` cookie containing the signed state value for CSRF protection and storing the intended redirect URL after authentication. The cookie is HttpOnly, Secure, SameSite=Lax, and has a short expiry.
            schema:
              type: string
              example: cilogon_oauth_state=...signed_state_value...; Max-Age=600; Path=/; HttpOnly; Secure; SameSite=Lax
      500:
        description: Server configuration error or internal failure during login initiation.
        content:
          application/json:
            schema:
              type: object
              properties:
                success:
                  type: boolean
                  example: False
                comment:
                  type: string
                  example: Server configuration error - CILogon metadata not loaded.
    """
    try:
        # 1. Create authorization URL and state using Authlib
        authorization_endpoint = cilogon_client.metadata.get("authorization_endpoint")

        # If the metadata wasn't loaded correctly at startup, this is a server configuration issue
        if not authorization_endpoint:
            print(
                "Error: CILogon authorization_endpoint not found in metadata. Check CILOGON_METADATA_URL."
            )
            return response.status(500).json(
                {
                    "success": False,
                    "comment": "Server configuration error: CILogon metadata not loaded.",
                }
            )

        uri, state = cilogon_client.create_authorization_url(authorization_endpoint)

        # 2. Prepare data to be stored in the cookie
        state_data_to_sign = {"state": state, "next_url": FRONTEND_URL}
        signed_state = sign_state_data(state_data_to_sign)

        # 3. Prepare cookie attributes
        # Max-Age is in seconds; Secure requires HTTPS; HttpOnly prevents JS access
        # SameSite=Lax is important for cross-site redirects
        cookie_attrs = [
            f"Max-Age={600}",  # e.g., 10 minutes
            "Path=/",
            "HttpOnly",
            "Secure",
            "SameSite=Lax",
        ]
        cookie_string = f"cilogon_oauth_state={signed_state}; {'; '.join(cookie_attrs)}"

        # 4. Return 302 Redirect response with Set-Cookie header
        response.body = {
            "statusCode": 302,
            "headers": {"Location": uri, "Set-Cookie": cookie_string},
        }
        response.terminated = True
        return event, response, {}

    except Exception as e:
        print(f"CILogon login error: {e}")
        response.status(500).json(
            {"success": False, "comment": "Login initiation failed"}
        )
        return event, response, {}

def get_or_create_external_user_identity(provider: str, provider_user_id: str, full_name: str, email: str = None) -> UserIdentity:
    """Gets a user given their SSO provider and user ID. If the user does not exist, it creates a new user with role 'pending'."""
    with user_identities_repository.create_session() as identity_session, \
         users_repository.create_session() as user_session:
        # Check if the identity already exists
        existing_identity: UserIdentity = identity_session.get_first({
            'provider': provider,
            'provider_user_id': provider_user_id
        })

        # If the identity exists, update the user record
        if existing_identity:
            linked_user = user_session.get_first({
                'id': existing_identity.user_id
            })
            user_session.update({
                'id': existing_identity.user_id,
                'full_name': full_name,
                'enabled': linked_user.enabled or False,
                'role': linked_user.role or 'user',
                'primary_email': email or linked_user.primary_email  
            })
            return existing_identity

        # Otherwise, create a new user with the provided identity and return it
        new_user = user_session.create({
            'full_name': full_name,
            'enabled': False, # New users are not enabled
            'role': 'user',
            'primary_email': email  # Primary email can be set later
        })

        identity_data = {
            'user_id': new_user['id'],
            'provider': provider,
            'provider_user_id': provider_user_id,
            'created_at': int(time.time())
        }
        identity_session.create(identity_data)

        return identity_session.get_first({
            'provider': provider,
            'provider_user_id': provider_user_id
        })

@route(
    "auth/cilogon/authorize", "GET"
)  # This path must match REDIRECT_URI in config.ini and CILogon setting
def cilogon_authorize(event, response: Response):
    """Handles the callback from CILogon after authentication.
    ---
    tags:
      - auth
      - cilogon
    description: Processes the authorization code and state returned by CILogon. Verifies the state against the cookie, fetches user information, creates an application-specific JWT, and redirects the user back to the frontend application with the token.
    parameters:
      - name: code
        in: query
        required: true
        description: The authorization code issued by CILogon upon successful user authentication.
        schema:
          type: string
      - name: state
        in: query
        required: true
        description: The opaque state value returned by CILogon. Used for CSRF protection verification against the value stored in the `cilogon_oauth_state` cookie.
        schema:
          type: string
    responses:
      302:
        description: Successful authentication and authorization. Redirects the user's browser back to the frontend application, embedding the application JWT in the URL fragment (#token=...). Clears the state cookie.
        headers:
          Location:
            description: The URL of the frontend application where the user should be redirected, including the application JWT in the fragment.
            schema:
              type: string
              format: uri
              example: https://your-frontend.com/#token=app_jwt_token
          Set-Cookie:
            description: Clears the `cilogon_oauth_state` cookie as it is no longer needed.
            schema:
              type: string
              example: cilogon_oauth_state=deleted; Max-Age=0; Path=/; HttpOnly; Secure; SameSite=Lax
      400:
        description: Bad request due to missing parameters, state mismatch (CSRF suspected), or invalid/expired state cookie.
        content:
          application/json:
            schema:
              type: object
              properties:
                success:
                  type: boolean
                  example: False
                comment:
                  type: string
                  example: Missing state, code, or cookie
      500:
        description: Internal server error during callback processing (e.g., failure to exchange code, fetch user info, or create JWT).
        content:
          application/json:
            schema:
              type: object
              properties:
                success:
                  type: boolean
                  example: False
                comment:
                  type: string
                  example: Authentication failed during callback
    """
    try:
        # 1. Extract state from query params and cookie from headers
        query_params = event.get("queryStringParameters", {})
        returned_state = query_params.get("state")
        code = query_params.get("code")  # Authorization code from CILogon

        headers = event.get("headers", {})
        cookie_header = headers.get("cookie")
        signed_state_from_cookie = None
        if cookie_header:
            cookies = {
                c.split("=")[0].strip(): c.split("=", 1)[1].strip()
                for c in cookie_header.split(";")
                if "=" in c
            }
            signed_state_from_cookie = cookies.get("cilogon_oauth_state")

        # 2. Validate inputs
        if not returned_state or not code or not signed_state_from_cookie:
            response.status(400).json(
                {
                    "success": False, 
                    "comment": "Missing state, code, or cookie. Please try logging in again from the dashboard."
                }
            )
            return event, response, {}

        # 3. Verify signed cookie data
        state_data = verify_signed_state_data(signed_state_from_cookie)
        if not state_data:
            response.status(400).json(
                {"success": False, "comment": "Invalid or expired state cookie"}
            )
            return event, response, {}

        # 4. Verify state parameter against cookie (CSRF check)
        original_state = state_data.get("state")
        next_url = state_data.get("next_url", "/")  # Default redirect if missing

        if original_state != returned_state:
            response.status(400).json(
                {"success": False, "comment": "State mismatch (CSRF suspected)"}
            )
            return event, response, {}

        # 5. Exchange code for access token (using OIDC token endpoint)
        token_endpoint = cilogon_client.metadata.get("token_endpoint")

        cilogon_client.fetch_token(
            url=token_endpoint,
            code=code,
            redirect_uri=REDIRECT_URI,
        )

        # 6. Get user info (using OIDC userinfo endpoint)
        userinfo_endpoint = cilogon_client.metadata.get("userinfo_endpoint")
        userinfo = cilogon_client.get(userinfo_endpoint).json()
        
        print(f"Obtained userinfo: {userinfo}")
        if not userinfo or "email" not in userinfo:
            response.status(400).json(
                {"success": False, "comment": "User info missing email field"}
            )
            return event, response, {}
        
        # 7. Get the user or create a new one in our application with the SSO provider
        app_user_identity = get_or_create_external_user_identity(
            provider="cilogon",
            provider_user_id=userinfo.get("sub"),
            full_name=userinfo.get("name"),
            email=userinfo.get("email")
        )
        
        # TODO: Check if user is 'deactivated' in CILogon (See POC for details)

        # 8. Create a JWT token for our application
        app_token = jwt.JsonWebToken.from_identity(app_user_identity).token

        # 8. Prepare final redirect response to the frontend
        #    Include app token in the redirect URL (e.g., fragment #token=...)
        final_redirect_url = f"{next_url}#token={app_token}"

        # Clear the temporary state cookie
        clear_cookie_attrs = [
            "Max-Age=0",  # Expire immediately
            "Path=/",
            "HttpOnly",
            "Secure",
            "SameSite=Lax",
        ]
        clear_cookie_string = (
            f"cilogon_oauth_state=deleted; {'; '.join(clear_cookie_attrs)}"
        )

        response.body = {
            "statusCode": 302,
            "headers": {
                "Location": final_redirect_url,
                "Set-Cookie": clear_cookie_string,
            },
        }
        response.terminated = True
        return event, response, {}

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"CILogon authorize error: {e}")
        # Log detailed error, including Authlib errors if possible
        # Consider redirecting to an error page on the frontend
        response.status(500).json(
            {"success": False, "comment": "Authentication failed during callback"}
        )
        return event, response, {}
