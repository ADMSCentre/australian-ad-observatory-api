from dataclasses import dataclass
from typing import Mapping
import os
from dotenv import load_dotenv

@dataclass
class AwsConfig:
    access_key_id: str
    secret_access_key: str
    region: str
    session_token: str | None = None
    
@dataclass
class DeploymentConfig:
    lambda_function_name: str
    zip_file: str
    deployment_bucket: str
    
@dataclass
class JwtConfig:
    secret: str
    expiration: int

@dataclass
class ApiKeyConfig:
    salt: str
    
@dataclass
class OpenSearchConfig:
    endpoint: str
    
@dataclass
class PostgresConfig:
    host: str
    port: int
    database: str
    username: str
    password: str
    
@dataclass
class CilogonConfig:
    client_id: str
    client_secret: str
    metadata_url: str
    redirect_uri: str
    
@dataclass
class AppConfig:
    state_cookie_secret: str
    salt: str
    frontend_url: str

@dataclass
class ExternalApiConfig:
    ad_delete_lambda_key: str
    
@dataclass
class BucketsConfig:
    observations: str
    metadata: str
    
@dataclass
class TestConfig:
    username: str
    password: str
    
@dataclass
class Config:
    aws: AwsConfig
    deployment: DeploymentConfig
    jwt: JwtConfig
    api_key: ApiKeyConfig
    open_search: OpenSearchConfig
    postgres: PostgresConfig
    cilogon: CilogonConfig
    app: AppConfig
    external_api: ExternalApiConfig
    buckets: BucketsConfig
    test: TestConfig

def _get_str(values: Mapping[str, str | None], key: str, default: str = '') -> str:
    """Helper to safely get string values from mapping."""
    value = values.get(key)
    return str(value) if value else default

def _create_config_from_dict(values: Mapping[str, str | None]) -> Config:
    return Config(
        aws=AwsConfig(
            access_key_id=_get_str(values, 'AWS_ACCESS_KEY_ID'),
            secret_access_key=_get_str(values, 'AWS_SECRET_ACCESS_KEY'),
            region=_get_str(values, 'AWS_REGION'),
            session_token=values.get('AWS_SESSION_TOKEN', None)
        ),
        deployment=DeploymentConfig(
            lambda_function_name=_get_str(values, 'DEPLOYMENT_LAMBDA_FUNCTION_NAME'),
            zip_file=_get_str(values, 'DEPLOYMENT_ZIP_FILE'),
            deployment_bucket=_get_str(values, 'DEPLOYMENT_BUCKET')
        ),
        jwt=JwtConfig(
            secret=_get_str(values, 'JWT_SECRET'),
            expiration=int(_get_str(values, 'JWT_EXPIRATION', '86400'))
        ),
        api_key=ApiKeyConfig(
            salt=_get_str(values, 'API_KEY_SALT')
        ),
        open_search=OpenSearchConfig(
            endpoint=_get_str(values, 'OPEN_SEARCH_ENDPOINT')
        ),
        postgres=PostgresConfig(
            host=_get_str(values, 'POSTGRES_HOST'),
            port=int(_get_str(values, 'POSTGRES_PORT', '5432')),
            database=_get_str(values, 'POSTGRES_DATABASE'),
            username=_get_str(values, 'POSTGRES_USERNAME'),
            password=_get_str(values, 'POSTGRES_PASSWORD')
        ),
        cilogon=CilogonConfig(
            client_id=_get_str(values, 'CILOGON_CLIENT_ID'),
            client_secret=_get_str(values, 'CILOGON_CLIENT_SECRET'),
            metadata_url=_get_str(values, 'CILOGON_METADATA_URL'),
            redirect_uri=_get_str(values, 'CILOGON_REDIRECT_URI')
        ),
        app=AppConfig(
            state_cookie_secret=_get_str(values, 'APP_STATE_COOKIE_SECRET'),
            salt=_get_str(values, 'APP_SALT'),
            frontend_url=_get_str(values, 'APP_FRONTEND_URL')
        ),
        external_api=ExternalApiConfig(
            ad_delete_lambda_key=_get_str(values, 'EXTERNAL_API_AD_DELETE_LAMBDA_KEY')
        ),
        buckets=BucketsConfig(
            observations=_get_str(values, 'BUCKETS_OBSERVATIONS'),
            metadata=_get_str(values, 'BUCKETS_METADATA')
        ),
        test=TestConfig(
            username=_get_str(values, 'TEST_USERNAME'),
            password=_get_str(values, 'TEST_PASSWORD')
        )
    )

def from_env_string(env_string: str) -> Config:
    """Load config from a .env format string."""
    import io
    from dotenv import dotenv_values
    values = dotenv_values(stream=io.StringIO(env_string))
    return _create_config_from_dict(values)

load_dotenv(verbose=True)
print("Loading configuration for environment:", os.getenv('ENV', 'unknown'))

config = _create_config_from_dict(os.environ)
