from db.clients.rds_storage_client import RdsStorageClient
from db.repository import Repository
from middlewares.authorise import Role, authorise
from models.attribute import AdAttribute, AdAttributeORM
from routes import route
from middlewares.authenticate import authenticate
from utils import use
import boto3
from config import config
import time

ad_attributes_repository = Repository(
    model=AdAttribute,
    keys=['observation_id', 'key'],
    auto_generate_key=False,
    client=RdsStorageClient(
        base_orm=AdAttributeORM
    )
)

session = boto3.Session(region_name='ap-southeast-2')

ADS_BUCKET = config.buckets.observations
AD_ATTRIBUTES_PREFIX = 'ad-custom-attributes'

@route('ads/{observer_id}/{timestamp}.{ad_id}/attributes', 'GET')
@use(authenticate)
def get_attributes(event, response):
    """Retrieve all attributes for an ad.

    Retrieve all custom attributes for the specified ad.
    ---
    tags:
        - ads/attributes
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            ad_id:
                                type: string
                            observer:
                                type: string
                            timestamp:
                                type: integer
                            attributes:
                                type: object
                                additionalProperties:
                                    type: object
                                    properties:
                                        value:
                                            type: string
                                        created_at:
                                            type: integer
                                        created_by:
                                            type: string
                                        modified_at:
                                            type: integer
                                        modified_by:
                                            type: string
        400:
            description: A failed response
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
                                example: 'ERROR_RETRIEVING_ATTRIBUTES'
    """
    ad_id = event['pathParameters']['ad_id']
    observer_id = event['pathParameters']['observer_id']
    timestamp = event['pathParameters']['timestamp']
    observation_id = f"{observer_id}_{timestamp}.{ad_id}"
    
    try:
        with ad_attributes_repository.create_session() as session:
            attributes = session.get({"observation_id": observation_id})
            if attributes is None:
                attributes = []
            
            attributes_dict = {}
            for attr in attributes:
                attributes_dict[attr.key] = {
                    "value": attr.value,
                    "created_at": attr.created_at,
                    "created_by": attr.created_by,
                    "modified_at": attr.modified_at,
                    "modified_by": attr.modified_by
                }
        
        return {
            "ad_id": ad_id,
            "observer": observer_id,
            "timestamp": int(timestamp),
            "attributes": attributes_dict
        }
    except Exception as e:
        return response.status(400).json({
            "success": False,
            "comment": "ERROR_RETRIEVING_ATTRIBUTES"
        })

def handle_update_hidden(observer_id, timestamp, ad_id, hidden):
    import requests
    action = None
    if hidden.lower() == "true":
        action = "DISABLE_AD"
    elif hidden.lower() == "false":
        action = "DISABLE_AD"
    else:
        raise ValueError("Invalid hidden value:", hidden)
    
    response = requests.post(
        "https://bxxqvaozhe237ak5ndca2zftz40kvgfm.lambda-url.ap-southeast-2.on.aws/", 
        json={
        "action": action,
        "observer_uuid": observer_id,
        "rdo_uuid_unsplit" : f"{timestamp}.{ad_id}"
        }
    )
    if response.status_code != 200:
        raise Exception("Failed to update ad hidden status with error:", response.text)
    

@route('ads/{observer_id}/{timestamp}.{ad_id}/attributes', 'PUT')
@use(authenticate)
@use(authorise(Role.USER, Role.ADMIN))
def add_attribute(event, response):
    """Add or update ad attributes in the database.

    Add or update custom attributes for the specified ad in the database.
    ---
    tags:
        - ads/attributes
    requestBody:
        required: true
        content:
            application/json:
                schema:
                    type: object
                    properties:
                        attribute:
                            type: object
                            properties:
                                key:
                                    type: string
                                value:
                                    type: string
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            comment:
                                type: string
        400:
            description: A failed response
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
                                example: 'AD_NOT_FOUND'
    """
    observer_id = event['pathParameters']['observer_id']
    timestamp = event['pathParameters']['timestamp']
    ad_id = event['pathParameters']['ad_id']
    attribute = event['body']['attribute']
    observation_id = f"{observer_id}_{timestamp}.{ad_id}"
    
    # Ensure the ad actually exists in the S3 bucket
    ad_path = f'{observer_id}/temp/{timestamp}.{ad_id}/'
    s3 = session.client('s3')
    try:
        # Ensure the folder exists
        s3.list_objects_v2(Bucket=ADS_BUCKET, Prefix=ad_path)
    except Exception as e:
        return response.status(400).json({
            "success": False,
            "comment": "AD_NOT_FOUND"
        })
    
    key = attribute['key']
    current_time = int(time.time())
    user_id = event['user'].id
    
    try:
        current_attr = None
        with ad_attributes_repository.create_session() as repository_session:
            # Get the current attribute if available
            current_attr = repository_session.get_first({
                "observation_id": observation_id,
                "key": key
            })
            # Create or update attribute
            new_attr = AdAttribute(
                observation_id=observation_id,
                key=key,
                value=str(attribute['value']),
                created_at=current_attr.created_at if current_attr else current_time,
                created_by=current_attr.created_by if current_attr else user_id,
                modified_at=current_time,
                modified_by=user_id
            )
            repository_session.create_or_update(new_attr)
            
        # Special handling for "hidden" attribute
        if key == "hidden":
            handle_update_hidden(observer_id, timestamp, ad_id, str(attribute['value']))
        
        return {
            "success": True,
            "comment": "ATTRIBUTE_SET_SUCCESSFULLY"
        }
    except Exception as e:
        return response.status(400).json({
            "success": False,
            "comment": "ERROR_SETTING_ATTRIBUTE: " + str(e)
        })

@route('ads/{observer_id}/{timestamp}.{ad_id}/attributes/{attribute_key}', 'GET')
@use(authenticate)
def get_single_attribute(event, response):
    """Retrieve a single attribute for an ad.

    Retrieve a specific custom attribute for the specified ad.
    ---
    tags:
        - ads/attributes
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            key:
                                type: string
                            value:
                                type: string
                            created_at:
                                type: integer
                            created_by:
                                type: string
                            modified_at:
                                type: integer
                            modified_by:
                                type: string
        400:
            description: A failed response
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
                                example: 'ATTRIBUTE_NOT_FOUND'
    """
    observer_id = event['pathParameters']['observer_id']
    timestamp = event['pathParameters']['timestamp']
    ad_id = event['pathParameters']['ad_id']
    attribute_key = event['pathParameters']['attribute_key']
    observation_id = f"{observer_id}_{timestamp}.{ad_id}"
    
    try:
        with ad_attributes_repository.create_session() as session:
            attr = session.get_first({
                "observation_id": observation_id,
                "key": attribute_key
            })
            if attr:
                return attr.model_dump()
            else:
                return response.status(400).json({
                    "success": False,
                    "comment": "ATTRIBUTE_NOT_FOUND"
                })
    except Exception as e:
        return response.status(400).json({
            "success": False,
            "comment": "ERROR_RETRIEVING_ATTRIBUTE"
        })

@route('ads/{observer_id}/{timestamp}.{ad_id}/attributes/{attribute_key}', 'DELETE')
@use(authenticate)
@use(authorise(Role.USER, Role.ADMIN))
def delete_properties(event, response):
    """Delete ad attributes from the database.

    Delete custom attributes for the specified ad in the database.
    ---
    tags:
        - ads/attributes
    responses:
        200:
            description: A successful response
            content:
                application/json:
                    schema:
                        type: object
                        properties:
                            success:
                                type: boolean
                            comment:
                                type: string
        400:
            description: A failed response
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
                                example: 'ERROR_DELETING_ATTRIBUTE'
    """
    observer_id = event['pathParameters']['observer_id']
    timestamp = event['pathParameters']['timestamp']
    ad_id = event['pathParameters']['ad_id']
    attribute_key = event['pathParameters']['attribute_key']
    observation_id = f"{observer_id}_{timestamp}.{ad_id}"
    
    try:
        with ad_attributes_repository.create_session() as session:
            attr = session.get_first({
                "observation_id": observation_id,
                "key": attribute_key
            })
            if attr:
                session.delete(attr)
                return {
                    "success": True,
                    "comment": "ATTRIBUTE_DELETED"
                }
            else:
                return response.status(400).json({
                    "success": False,
                    "comment": "ATTRIBUTE_NOT_FOUND"
                })
    except Exception as e:
        return response.status(400).json({
            "success": False,
            "comment": "ERROR_DELETING_ATTRIBUTE"
        })