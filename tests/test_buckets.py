"""Integration tests for bucket operations."""

import pytest
from buckerio import Buckerio, BucketNotFoundError, BucketAlreadyExistsError


@pytest.mark.integration
class TestBucketOperations:
    """Test bucket operations against a running Buckerio server."""

    def test_create_and_delete_bucket(self, client: Buckerio) -> None:
        """Test creating and deleting a bucket."""
        bucket_name = "test-create-delete"

        # Cleanup first
        try:
            client.delete_bucket(bucket_name)
        except BucketNotFoundError:
            pass

        # Create
        client.create_bucket(bucket_name)
        assert client.bucket_exists(bucket_name)

        # Delete
        client.delete_bucket(bucket_name)
        assert not client.bucket_exists(bucket_name)

    def test_list_buckets(self, client: Buckerio, test_bucket: str) -> None:
        """Test listing buckets."""
        buckets = client.list_buckets()
        bucket_names = [b.name for b in buckets]
        assert test_bucket in bucket_names

    def test_bucket_exists(self, client: Buckerio, test_bucket: str) -> None:
        """Test bucket existence check."""
        assert client.bucket_exists(test_bucket)
        assert not client.bucket_exists("nonexistent-bucket-xyz")

    def test_create_duplicate_bucket(self, client: Buckerio, test_bucket: str) -> None:
        """Test creating a bucket that already exists."""
        with pytest.raises(BucketAlreadyExistsError):
            client.create_bucket(test_bucket)

    def test_delete_nonexistent_bucket(self, client: Buckerio) -> None:
        """Test deleting a bucket that doesn't exist."""
        with pytest.raises(BucketNotFoundError):
            client.delete_bucket("nonexistent-bucket-xyz")
