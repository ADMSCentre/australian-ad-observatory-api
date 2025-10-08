from time import sleep
from base import execute_endpoint
import requests
import pytest
from config import config
from lambda_function import lambda_handler as local_handler
import json
# Needed for importing the lambda_function module
import sys

from utils.opensearch.rdo_open_search import RdoOpenSearch
sys.path.append("../")

username = config.test.username
password = config.test.password


@pytest.mark.skip(reason="Helper function, not a test")
def get_login_token():
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


def test_get_ads():
    token = get_login_token()
    headers = {
        'Authorization': f'Bearer {token}'
    }
    respones = local_handler({
        'path': '/ads',
        'httpMethod': 'GET',
        'headers': headers
    }, None)
    print(respones)
    assert respones['statusCode'] == 200, f"Expected 200, got {respones['statusCode']}"
    body: dict = json.loads(respones['body'])
    presigned_url = body.get('presigned_url')
    # Check if presigned_url is a string
    assert isinstance(
        presigned_url, str), f"Expected string, got {type(presigned_url)}"

    # Fetch the presigned URL
    response = requests.get(presigned_url)
    # Check if the response is successful
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    # Check if the content is a list
    ads = response.json()
    assert isinstance(ads, list), f"Expected list, got {type(ads)}"
    print(f"Number of ads: {len(ads)}")


def test_get_recent_ads_by_observer():
    token = get_login_token()
    headers = {
        'Authorization': f'Bearer {token}'
    }
    observer_id = "f7d8de6e-77e9-419e-82a4-b7f833a981cc"
    response = local_handler({
        'path': f'/ads/{observer_id}/recent',
        'httpMethod': 'GET',
        'headers': headers,
    }, None)
    print(response)
    assert response['statusCode'] == 200, f"Expected 200, got {response['statusCode']}"
    body: dict = json.loads(response['body'])
    ads = body.get('ads', [])
    # Check if ads is a list
    assert isinstance(ads, list), f"Expected list, got {type(ads)}"
    print(f"Number of recent ads for observer {observer_id}: {len(ads)}")


def ensure_ad_indexed(observer_id: str, timestamp: int, ad_id: str):
    response = execute_endpoint(
        endpoint=f'/ads/{observer_id}/{timestamp}.{ad_id}/request_index',
        method='GET',
        auth=True
    )
    assert response['statusCode'] == 200, f"Expected 200, got {response['statusCode']}"
    body: dict = response['body']
    assert body.get(
        'success') is True, f"Expected success True, got {body.get('success')}"


def ensure_ad_unindexed(observer_id: str, timestamp: int, ad_id: str):
    response = execute_endpoint(
        endpoint=f'/ads/{observer_id}/{timestamp}.{ad_id}/request_unindex',
        method='GET',
        auth=True
    )
    assert response['statusCode'] == 200, f"Expected 200, got {response['statusCode']}"
    body: dict = response['body']
    assert body.get(
        'success') is True, f"Expected success True, got {body.get('success')}"


def test_request_index():
    observer_id = "c1a56f0c-8775-4b5e-bc7e-8b9f41039cd5"
    timestamp = 1753328035448
    ad_id = "4c747fca-2d38-4694-a865-bce39e5dc011"
    response = execute_endpoint(
        endpoint=f'/ads/{observer_id}/{timestamp}.{ad_id}/request_index',
        method='GET',
        auth=True
    )
    print(response)
    assert response['statusCode'] == 200, f"Expected 200, got {response['statusCode']}"
    body: dict = response['body']
    print(body)
    assert body.get(
        'success') is True, f"Expected success True, got {body.get('success')}"

    # Ensure that the ad can be found in observer's ads
    response = execute_endpoint(
        endpoint=f'/ads/{observer_id}',
        method='GET',
        auth=True
    )
    ads = set(response['body'].get('ads', []))
    ad_path = f"{observer_id}/temp/{timestamp}.{ad_id}"
    print(ads)
    assert ad_path in ads, f"Ad {ad_path} should be in observer's ads"


def test_request_unindex():
    observer_id = "c1a56f0c-8775-4b5e-bc7e-8b9f41039cd5"
    timestamp = 1753328026200
    ad_id = "47dbac57-3742-455a-9018-8aeeff92326f"

    response = execute_endpoint(
        endpoint=f'/ads/{observer_id}/{timestamp}.{ad_id}/request_unindex',
        method='GET',
        auth=True
    )
    print(response)
    assert response['statusCode'] == 200, f"Expected 200, got {response['statusCode']}"
    body: dict = response['body']
    assert body.get(
        'success') is True, f"Expected success True, got {body.get('success')}"

    # Ensure that the ad cannot be found in observer's ads
    response = execute_endpoint(
        endpoint=f'/ads/{observer_id}',
        method='GET',
        auth=True
    )
    ads = set(response['body'].get('ads', []))
    ad_path = f"{observer_id}/temp/{timestamp}.{ad_id}"
    assert ad_path not in ads, f"Ad {ad_path} should not be in observer's ads"

    # Restore the ad to the original state for idempotency
    ensure_ad_indexed(observer_id, timestamp, ad_id)
