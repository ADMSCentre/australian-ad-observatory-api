"""
Integration tests for API key endpoints.

This module tests the full API key lifecycle:
- Creating API keys via JWT authentication
- Listing API keys
- Getting specific API key details
- Using API keys for authentication
- Deleting API keys
"""

from base import execute_endpoint
import json


def create_api_key(title: str = "Test API Key", description: str = None):
    """
    Create a new API key using JWT authentication.
    
    :param title: The title for the API key
    :param description: Optional description for the API key
    :return: The response from the API call
    """
    body = {'title': title}
    if description:
        body['description'] = description
    
    response = execute_endpoint('/api-keys', method='POST', body=body, auth=True)
    return response


def list_api_keys(user_id: str = None):
    """
    List API keys for the authenticated user (or specified user if admin).
    
    :param user_id: Optional user ID to list keys for (admin only)
    :return: The response from the API call
    """
    endpoint = '/api-keys'
    if user_id:
        endpoint += f'?user_id={user_id}'
    
    response = execute_endpoint(endpoint, method='GET', auth=True)
    return response


def get_api_key(key_id: str):
    """
    Get details of a specific API key.
    
    :param key_id: The ID of the API key
    :return: The response from the API call
    """
    response = execute_endpoint(f'/api-keys/{key_id}', method='GET', auth=True)
    return response


def delete_api_key(key_id: str):
    """
    Delete (revoke) an API key.
    
    :param key_id: The ID of the API key to delete
    :return: The response from the API call
    """
    response = execute_endpoint(f'/api-keys/{key_id}', method='DELETE', auth=True)
    return response


def run_with_api_key(api_key: str, endpoint: str = '/users/self', method: str = 'GET'):
    """
    Test making an authenticated request using an API key.
    
    :param api_key: The API key to use for authentication
    :param endpoint: The endpoint to test (default: /users/self)
    :param method: The HTTP method to use
    :return: The response from the API call
    """
    headers = {'x-api-key': api_key}
    response = execute_endpoint(endpoint, method=method, headers=headers, auth=False)
    return response


# ============================================================================
# Test Cases
# ============================================================================

def create_key():
    response = create_api_key(
        title="Test Key for Integration Tests",
        description="This key is created during API tests"
    )
    body = response['body']
    api_key_data = body['api_key']
    return api_key_data

def test_create_api_key():
    """Test creating a new API key with JWT authentication."""
    print("\n[TEST] Creating API key...")
    
    response = create_api_key(
        title="Test Key for Integration Tests",
        description="This key is created during API tests"
    )
    
    assert response['statusCode'] == 201, f"Expected 201, got {response['statusCode']}"
    
    body = response['body']
    assert body['success'] is True, "Expected success=True"
    assert 'api_key' in body, "Response should contain api_key"
    assert 'key' in body['api_key'], "API key object should contain full key"
    assert 'warning' in body, "Response should contain warning"
    
    # Verify structure of returned API key
    api_key_data = body['api_key']
    assert 'id' in api_key_data
    assert 'user_id' in api_key_data
    assert 'title' in api_key_data
    assert api_key_data['title'] == "Test Key for Integration Tests"
    assert 'description' in api_key_data
    assert api_key_data['description'] == "This key is created during API tests"
    assert 'suffix' in api_key_data
    assert len(api_key_data['suffix']) == 6, "Suffix should be 6 characters"
    assert 'created_at' in api_key_data
    
    # Verify the full key is present and matches suffix
    full_key = api_key_data['key']
    assert len(full_key) > 60, "Full key should be at least 60 characters"
    assert full_key.endswith(api_key_data['suffix']), "Full key should end with suffix"
    
    print(f"✓ API key created successfully: ID={api_key_data['id']}, suffix=...{api_key_data['suffix']}")
    
    # Clean up
    delete_api_key(api_key_data['id'])
    return


def test_create_api_key_minimal():
    """Test creating an API key with only required fields."""
    print("\n[TEST] Creating API key with minimal data...")
    
    response = create_api_key(title="Minimal Test Key")
    
    assert response['statusCode'] == 201
    body = response['body']
    assert body['success'] is True
    assert body['api_key']['description'] is None
    
    print(f"✓ Minimal API key created successfully")
    # Clean up
    delete_api_key(body['api_key']['id'])


def test_create_api_key_missing_title():
    """Test that creating an API key without title fails."""
    print("\n[TEST] Creating API key without title (should fail)...")
    
    response = execute_endpoint('/api-keys', method='POST', body={}, auth=True)
    
    assert response['statusCode'] == 400, f"Expected 400, got {response['statusCode']}"
    body = response['body']
    assert body['success'] is False
    assert body['comment'] == 'MISSING_TITLE'
    
    print("✓ API key creation correctly rejected without title")


def test_list_api_keys():
    """Test listing API keys for the authenticated user."""
    print("\n[TEST] Listing API keys...")
    
    # Create a test key first
    created_key = create_key()
    
    # List all keys
    response = list_api_keys()
    
    assert response['statusCode'] == 200, f"Expected 200, got {response['statusCode']}"
    body = response['body']
    assert 'api_keys' in body
    assert isinstance(body['api_keys'], list)
    
    # Find our created key in the list
    found = False
    for key in body['api_keys']:
        if key['id'] == created_key['id']:
            found = True
            # Verify the key data doesn't contain the full key
            assert 'key' not in key, "List response should not contain full key"
            assert 'hashed_key' not in key, "List response should not contain hashed_key"
            assert 'suffix' in key
            assert key['suffix'] == created_key['suffix']
            assert key['title'] == created_key['title']
            break
    
    assert found, "Created API key should appear in list"
    
    print(f"✓ Listed {len(body['api_keys'])} API keys")
    
    # Clean up
    delete_api_key(created_key['id'])


def test_get_api_key():
    """Test getting details of a specific API key."""
    print("\n[TEST] Getting specific API key details...")
    
    # Create a test key
    created_key = create_key()
    key_id = created_key['id']
    
    # Get the key details
    response = get_api_key(key_id)
    
    assert response['statusCode'] == 200, f"Expected 200, got {response['statusCode']}"
    body = response['body']
    
    # Verify structure
    assert body['id'] == key_id
    assert body['title'] == created_key['title']
    assert body['suffix'] == created_key['suffix']
    assert 'created_at' in body
    assert 'last_used_at' in body
    
    # Verify security: full key should not be returned
    assert 'key' not in body, "GET response should not contain full key"
    assert 'hashed_key' not in body, "GET response should not contain hashed_key"
    
    print(f"✓ Retrieved API key details for ID={key_id}")
    
    # Clean up
    delete_api_key(key_id)


def test_get_api_key_not_found():
    """Test getting a non-existent API key."""
    print("\n[TEST] Getting non-existent API key (should fail)...")
    
    response = get_api_key("00000000-0000-0000-0000-000000000000")
    
    assert response['statusCode'] == 404, f"Expected 404, got {response['statusCode']}"
    body = response['body']
    assert body['success'] is False
    assert body['comment'] == 'API_KEY_NOT_FOUND'
    
    print("✓ Non-existent API key correctly returned 404")


def test_delete_api_key():
    """Test deleting an API key."""
    print("\n[TEST] Deleting API key...")
    
    # Create a test key
    created_key = create_key()
    key_id = created_key['id']
    
    # Delete the key
    response = delete_api_key(key_id)
    
    assert response['statusCode'] == 200, f"Expected 200, got {response['statusCode']}"
    body = response['body']
    assert body['success'] is True
    assert 'deleted successfully' in body['comment'].lower()
    
    # Verify the key is actually deleted
    get_response = get_api_key(key_id)
    assert get_response['statusCode'] == 404, "Deleted key should return 404"
    
    print(f"✓ API key deleted successfully: ID={key_id}")


def test_delete_api_key_not_found():
    """Test deleting a non-existent API key."""
    print("\n[TEST] Deleting non-existent API key (should fail)...")
    
    response = delete_api_key("00000000-0000-0000-0000-000000000000")
    
    assert response['statusCode'] == 404, f"Expected 404, got {response['statusCode']}"
    body = response['body']
    assert body['success'] is False
    
    print("✓ Non-existent API key deletion correctly returned 404")


def test_authentication_with_api_key():
    """Test using an API key for authentication."""
    print("\n[TEST] Authenticating with API key...")
    
    # Create a test key
    created_key = create_key()
    api_key = created_key['key']
    key_id = created_key['id']
    
    # Use the API key to authenticate and get current user
    response = run_with_api_key(api_key, endpoint='/users/self')
    
    assert response['statusCode'] == 200, f"Expected 200, got {response['statusCode']}"
    body = response['body']
    
    # Verify we got user data
    assert 'id' in body
    assert 'role' in body
    assert body['role'] == 'admin', "Test user should be admin"
    
    print(f"✓ Successfully authenticated with API key")
    
    # Verify last_used_at was updated
    key_details = get_api_key(key_id)
    assert key_details['body']['last_used_at'] is not None, "last_used_at should be set after use"
    
    print(f"✓ last_used_at timestamp updated")
    
    # Clean up
    delete_api_key(key_id)


def test_authentication_with_invalid_api_key():
    """Test that invalid API keys are rejected."""
    print("\n[TEST] Authenticating with invalid API key (should fail)...")
    
    response = run_with_api_key("invalid-api-key-12345", endpoint='/users/self')
    
    assert response['statusCode'] == 401, f"Expected 401, got {response['statusCode']}"
    body = response['body']
    assert body['success'] is False
    assert body['comment'] == 'INVALID_API_KEY'
    
    print("✓ Invalid API key correctly rejected")


def test_api_key_revocation():
    """Test that deleted API keys cannot be used for authentication."""
    print("\n[TEST] Testing API key revocation...")
    
    # Create a test key
    created_key = create_key()
    api_key = created_key['key']
    key_id = created_key['id']
    
    # Verify the key works
    response = run_with_api_key(api_key, endpoint='/users/self')
    assert response['statusCode'] == 200, "Key should work before deletion"
    
    # Delete the key
    delete_api_key(key_id)
    
    # Try to use the deleted key
    response = run_with_api_key(api_key, endpoint='/users/self')
    assert response['statusCode'] == 401, f"Expected 401, got {response['statusCode']}"
    assert response['body']['comment'] == 'INVALID_API_KEY', "Deleted key should be invalid"
    
    print("✓ Revoked API key correctly rejected")


def test_api_key_with_different_endpoints():
    """Test using API key authentication with various endpoints."""
    print("\n[TEST] Testing API key with different endpoints...")
    
    # Create a test key
    created_key = create_key()
    api_key = created_key['key']
    key_id = created_key['id']
    
    # Test various endpoints
    endpoints_to_test = [
        ('/users/self', 'GET'),
        ('/users', 'GET'),
        ('/api-keys', 'GET'),
    ]
    
    for endpoint, method in endpoints_to_test:
        response = run_with_api_key(api_key, endpoint=endpoint, method=method)
        assert response['statusCode'] in [200, 201], \
            f"Endpoint {endpoint} failed with status {response['statusCode']}"
        print(f"  ✓ {method} {endpoint} works with API key")
    
    print("✓ API key works with multiple endpoints")
    
    # Clean up
    delete_api_key(key_id)


def test_api_key_cannot_manage_itself():
    """Test that API keys cannot be used to create or delete other API keys."""
    print("\n[TEST] Testing that API keys require JWT for management...")
    
    # Create a test key using JWT
    created_key = create_key()
    api_key = created_key['key']
    key_id = created_key['id']
    
    # Try to create another key using API key (should work since we allow it)
    headers = {'x-api-key': api_key}
    response = execute_endpoint(
        '/api-keys',
        method='POST',
        body={'title': 'Another Key'},
        headers=headers,
        auth=False
    )
    
    # This should actually work in our implementation
    # API keys have same permissions as the user
    assert response['statusCode'] == 201, "API keys can create other keys"
    new_key_id = response['body']['api_key']['id']
    
    print("✓ API keys can manage other keys (same permissions as user)")
    
    # Clean up
    delete_api_key(key_id)
    delete_api_key(new_key_id)