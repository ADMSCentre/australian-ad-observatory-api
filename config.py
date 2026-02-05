"""Configuration module using environment variables.

All configuration is loaded from environment variables. Use a .env file for local development.
The module automatically loads environment variables from a .env file if present.
"""
import os
import warnings
from dataclasses import dataclass
from typing import Optional

# Load .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# Track missing environment variables
_missing_required_vars = []
_missing_optional_vars = []


def get_env(key: str, default: str = '', required: bool = False) -> str:
    """Get environment variable with optional default and required validation.
    
    Args:
        key: Environment variable name
        default: Default value if not set
        required: If True, raises error if not set. If False, logs warning.
        
    Returns:
        Environment variable value or default
        
    Raises:
        ValueError: If required and not set
    """
    value = os.getenv(key, default)
    
    if not value:
        if required:
            _missing_required_vars.append(key)
        elif default == '' and not required:
            # Only track optional vars that don't have defaults
            _missing_optional_vars.append(key)
    
    return value


def get_env_int(key: str, default: int = 0) -> int:
    """Get environment variable as integer.
    
    Args:
        key: Environment variable name
        default: Default value if not set
        
    Returns:
        Environment variable as integer or default
    """
    value = os.getenv(key)
    if value is None:
        return default
    return int(value)


@dataclass
class AwsConfig:
    access_key_id: str
    secret_access_key: str
    region: str


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
    
    @property
    def url(self) -> str:
        """Generate database URL from config."""
        return f'postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}'


@dataclass
class BucketsConfig:
    observations: str
    metadata: str


@dataclass
class Config:
    aws: AwsConfig
    open_search: OpenSearchConfig
    postgres: PostgresConfig
    buckets: BucketsConfig


def _validate_config() -> None:
    """Validate configuration and emit warnings/errors for missing variables.
    
    Raises:
        ValueError: If critical environment variables are missing
    """
    # Define critical variables that must be set for production use
    critical_vars = {
        'AWS_ACCESS_KEY_ID': 'AWS credentials for S3 access',
        'AWS_SECRET_ACCESS_KEY': 'AWS credentials for S3 access',
        'OPENSEARCH_ENDPOINT': 'OpenSearch cluster connection',
        'POSTGRES_PASSWORD': 'PostgreSQL database authentication',
    }
    
    missing_critical = []
    for var, purpose in critical_vars.items():
        if var in _missing_optional_vars:
            missing_critical.append(f"{var} ({purpose})")
    
    if missing_critical:
        error_msg = (
            "Missing critical environment variables required for operation:\n  - " +
            "\n  - ".join(missing_critical) +
            "\n\nPlease set these variables in your .env file before running scripts."
        )
        raise ValueError(error_msg)
    
    if _missing_optional_vars:
        warning_msg = (
            "Missing optional environment variables (using defaults):\n  - " +
            "\n  - ".join(_missing_optional_vars)
        )
        warnings.warn(warning_msg, category=UserWarning, stacklevel=3)


def load_config() -> Config:
    """Load configuration from environment variables.
    
    Emits warnings for missing optional variables and raises errors for missing critical ones.
    
    Raises:
        ValueError: If critical environment variables are missing
    """
    config = Config(
        aws=AwsConfig(
            access_key_id=get_env('AWS_ACCESS_KEY_ID'),
            secret_access_key=get_env('AWS_SECRET_ACCESS_KEY'),
            region=get_env('AWS_REGION', 'ap-southeast-2')
        ),
        open_search=OpenSearchConfig(
            endpoint=get_env('OPENSEARCH_ENDPOINT')
        ),
        postgres=PostgresConfig(
            host=get_env('POSTGRES_HOST', 'localhost'),
            port=get_env_int('POSTGRES_PORT', 5432),
            database=get_env('POSTGRES_DATABASE', 'aao'),
            username=get_env('POSTGRES_USERNAME', 'postgres'),
            password=get_env('POSTGRES_PASSWORD')
        ),
        buckets=BucketsConfig(
            observations=get_env('S3_OBSERVATIONS_BUCKET', 'fta-mobile-observations-v2'),
            metadata=get_env('S3_METADATA_BUCKET')
        )
    )
    
    # Validate configuration
    _validate_config()
    
    return config


# Load config on module import
config = load_config()
