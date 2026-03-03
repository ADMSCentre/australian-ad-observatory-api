import boto3
import json

from config import config

session = boto3.Session()

class User:
    enabled: bool
    password: str
    username: str
    full_name: str

def get_dynamo_credentials() -> list[User]:
    # Current table: https://us-east-2.console.aws.amazon.com/dynamodbv2/home?region=us-east-2#item-explorer?maximize=true&operation=SCAN&table=fta_dashboard_credentials
    dynamodb = session.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table('fta_dashboard_credentials')
    response = table.scan()
    return response['Items']

def migrate_credentials_from_dynamo_to_s3():
    TARGET_BUCKET = config.buckets.metadata
    USERS_FOLDER_PREFIX = 'metadata/dashboard-users'
    user_data = get_dynamo_credentials()
    s3 = session.client('s3', region_name='ap-southeast-2')
    for user in user_data:
        user_id = user['username']
        user_data = json.dumps(user)
        print(f'Uploading {user_id} to {TARGET_BUCKET}')
        s3.put_object(
            Bucket=TARGET_BUCKET,
            Key=f'{USERS_FOLDER_PREFIX}/{user_id}/credentials.json',
            Body=user_data.encode('utf-8')
        )
    return True

def delete_s3_credentials():
    TARGET_BUCKET = config.buckets.metadata
    USERS_FOLDER_PREFIX = 'metadata/dashboard-users/'
    s3 = session.client('s3', region_name='ap-southeast-2')
    user_data = get_dynamo_credentials()
    for user in user_data:
        user_id = user['username']
        print(f'Deleting {user_id} from {TARGET_BUCKET}')
        s3.delete_object(
            Bucket=TARGET_BUCKET,
            Key=f'{USERS_FOLDER_PREFIX}/{user_id}/credentials.json'
        )
    return True

def main():
    migrate_credentials_from_dynamo_to_s3()
    # delete_s3_credentials()

if __name__ == "__main__":
    # main()
    pass