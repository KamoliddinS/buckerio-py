"""Pytest configuration and fixtures."""

import os
import pytest
from buckerio import Buckerio


@pytest.fixture
def endpoint() -> str:
    """Get Buckerio endpoint from env or use default."""
    return os.environ.get("BUCKERIO_ENDPOINT", "http://localhost:9000")


@pytest.fixture
def access_key() -> str:
    """Get access key from env or use default."""
    return os.environ.get("BUCKERIO_ACCESS_KEY", "admin")


@pytest.fixture
def secret_key() -> str:
    """Get secret key from env or use default."""
    return os.environ.get("BUCKERIO_SECRET_KEY", "admin")


@pytest.fixture
def client(endpoint: str, access_key: str, secret_key: str) -> Buckerio:
    """Create Buckerio client."""
    return Buckerio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
    )


@pytest.fixture
def test_bucket(client: Buckerio) -> str:
    """Create a test bucket and clean up after test."""
    bucket_name = "test-bucket-py"

    # Clean up if exists
    try:
        # Delete all objects first
        for obj in client.list_all_objects(bucket_name):
            client.delete_object(bucket_name, obj.key)
        client.delete_bucket(bucket_name)
    except Exception:
        pass

    # Create bucket
    client.create_bucket(bucket_name)

    yield bucket_name

    # Cleanup
    try:
        for obj in client.list_all_objects(bucket_name):
            client.delete_object(bucket_name, obj.key)
        client.delete_bucket(bucket_name)
    except Exception:
        pass
