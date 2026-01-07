"""Tests for the Buckerio client."""

import pytest
from buckerio import Buckerio, InvalidBucketNameError


class TestBuckerioClient:
    """Test Buckerio client initialization."""

    def test_init_default(self) -> None:
        """Test client with default values."""
        client = Buckerio()
        assert client is not None

    def test_init_with_credentials(self) -> None:
        """Test client with explicit credentials."""
        client = Buckerio(
            endpoint="http://localhost:9000",
            access_key="mykey",
            secret_key="mysecret",
        )
        assert client is not None

    def test_context_manager(self) -> None:
        """Test client as context manager."""
        with Buckerio() as client:
            assert client is not None

    def test_invalid_bucket_name_too_short(self) -> None:
        """Test validation of short bucket name."""
        client = Buckerio()
        with pytest.raises(InvalidBucketNameError) as exc_info:
            client.create_bucket("ab")
        assert "at least 3 characters" in str(exc_info.value)

    def test_invalid_bucket_name_too_long(self) -> None:
        """Test validation of long bucket name."""
        client = Buckerio()
        with pytest.raises(InvalidBucketNameError) as exc_info:
            client.create_bucket("a" * 64)
        assert "at most 63 characters" in str(exc_info.value)

    def test_invalid_bucket_name_uppercase(self) -> None:
        """Test validation of uppercase bucket name."""
        client = Buckerio()
        with pytest.raises(InvalidBucketNameError) as exc_info:
            client.create_bucket("MyBucket")
        assert "lowercase" in str(exc_info.value)

    def test_invalid_bucket_name_ip_address(self) -> None:
        """Test validation of IP address bucket name."""
        client = Buckerio()
        with pytest.raises(InvalidBucketNameError) as exc_info:
            client.create_bucket("192.168.1.1")
        assert "IP address" in str(exc_info.value)
