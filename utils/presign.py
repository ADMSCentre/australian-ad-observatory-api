"""Utility for replacing S3 URIs with pre-signed URLs in nested data structures.

Recursively walks dicts/lists and replaces any string value matching
``s3://<bucket>/<key>`` with a pre-signed ``GetObject`` URL (1-hour expiry).
Non-S3 strings are returned unchanged.
"""

from __future__ import annotations

from urllib.parse import urlparse

import boto3

from config import config

_PRESIGN_EXPIRY_SECONDS = 3600  # 1 hour

# Module-level S3 client (re-used across invocations within the same Lambda
# container).  Created lazily on first call to avoid import-time side-effects
# in test environments.
_s3_client = None


def _get_s3_client():
    """Return a cached boto3 S3 client for the ``ap-southeast-2`` region."""
    global _s3_client
    if _s3_client is None:
        session = boto3.Session(region_name="ap-southeast-2")
        _s3_client = session.client("s3")
    return _s3_client


def _presign_s3_uri(uri: str, client=None) -> str:
    """Convert a single ``s3://bucket/key`` URI to a pre-signed HTTPS URL.

    Parameters
    ----------
    uri:
        An S3 URI of the form ``s3://bucket-name/path/to/key``.
    client:
        Optional boto3 S3 client (used for testing). Falls back to the
        module-level cached client.

    Returns
    -------
    str
        A pre-signed HTTPS URL valid for ``_PRESIGN_EXPIRY_SECONDS``.
    """
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    if client is None:
        client = _get_s3_client()

    return client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=_PRESIGN_EXPIRY_SECONDS,
    )


def replace_s3_uris(data, *, client=None):
    """Recursively walk *data* and replace ``s3://`` strings with pre-signed URLs.

    Supported structures: ``dict``, ``list``, ``str``.  All other types are
    returned as-is.

    Parameters
    ----------
    data:
        The value to process — typically the JSONB ``data`` dict from an ORM
        model.
    client:
        Optional boto3 S3 client (for dependency injection in tests).

    Returns
    -------
    The same structure with every ``s3://…`` string replaced by a pre-signed
    HTTPS URL.
    """
    if isinstance(data, dict):
        return {k: replace_s3_uris(v, client=client) for k, v in data.items()}
    if isinstance(data, list):
        return [replace_s3_uris(item, client=client) for item in data]
    if isinstance(data, str) and data.startswith("s3://"):
        return _presign_s3_uri(data, client=client)
    return data
