"""Integration tests for object operations."""

import pytest
from buckerio import Buckerio, ObjectNotFoundError


@pytest.mark.integration
class TestObjectOperations:
    """Test object operations against a running Buckerio server."""

    def test_put_and_get_object(self, client: Buckerio, test_bucket: str) -> None:
        """Test uploading and downloading an object."""
        key = "test-file.txt"
        content = b"Hello, Buckerio!"

        # Upload
        result = client.put_object(test_bucket, key, content)
        assert result.etag

        # Download
        obj = client.get_object(test_bucket, key)
        assert obj.content == content
        assert obj.etag == result.etag

    def test_put_string_content(self, client: Buckerio, test_bucket: str) -> None:
        """Test uploading string content."""
        key = "string-file.txt"
        content = "String content!"

        result = client.put_object(test_bucket, key, content)
        assert result.etag

        obj = client.get_object(test_bucket, key)
        assert obj.content == content.encode("utf-8")

    def test_delete_object(self, client: Buckerio, test_bucket: str) -> None:
        """Test deleting an object."""
        key = "to-delete.txt"

        # Create
        client.put_object(test_bucket, key, b"delete me")
        assert client.object_exists(test_bucket, key)

        # Delete
        client.delete_object(test_bucket, key)
        assert not client.object_exists(test_bucket, key)

    def test_head_object(self, client: Buckerio, test_bucket: str) -> None:
        """Test getting object metadata."""
        key = "metadata-test.txt"
        content = b"Metadata test content"

        client.put_object(test_bucket, key, content)
        info = client.head_object(test_bucket, key)

        assert info.key == key
        assert info.size == len(content)
        assert info.etag is not None

    @pytest.mark.skip(reason="Server doesn't support custom metadata yet")
    def test_object_with_metadata(self, client: Buckerio, test_bucket: str) -> None:
        """Test object with custom metadata."""
        key = "custom-meta.txt"
        metadata = {"author": "test", "version": "1.0"}

        client.put_object(test_bucket, key, b"content", metadata=metadata)
        info = client.head_object(test_bucket, key)

        # Metadata keys are lowercased
        assert info.metadata.get("author") == "test"
        assert info.metadata.get("version") == "1.0"

    def test_list_objects(self, client: Buckerio, test_bucket: str) -> None:
        """Test listing objects."""
        # Create some objects
        keys = ["file1.txt", "file2.txt", "dir/file3.txt"]
        for key in keys:
            client.put_object(test_bucket, key, b"content")

        # List all
        result = client.list_objects(test_bucket)
        listed_keys = [obj.key for obj in result.objects]
        for key in keys:
            assert key in listed_keys

    def test_list_objects_with_prefix(self, client: Buckerio, test_bucket: str) -> None:
        """Test listing objects with prefix filter."""
        # Create objects
        client.put_object(test_bucket, "prefix/a.txt", b"a")
        client.put_object(test_bucket, "prefix/b.txt", b"b")
        client.put_object(test_bucket, "other/c.txt", b"c")

        # List with prefix
        result = client.list_objects(test_bucket, prefix="prefix/")
        keys = [obj.key for obj in result.objects]
        assert "prefix/a.txt" in keys
        assert "prefix/b.txt" in keys
        assert "other/c.txt" not in keys

    def test_list_all_objects_iterator(self, client: Buckerio, test_bucket: str) -> None:
        """Test iterating all objects."""
        # Create objects
        for i in range(5):
            client.put_object(test_bucket, f"iter-{i}.txt", b"content")

        # Iterate
        keys = [obj.key for obj in client.list_all_objects(test_bucket, prefix="iter-")]
        assert len(keys) == 5

    def test_get_nonexistent_object(self, client: Buckerio, test_bucket: str) -> None:
        """Test getting an object that doesn't exist."""
        with pytest.raises(ObjectNotFoundError):
            client.get_object(test_bucket, "nonexistent-key")

    def test_object_exists(self, client: Buckerio, test_bucket: str) -> None:
        """Test object existence check."""
        key = "exists-test.txt"

        assert not client.object_exists(test_bucket, key)

        client.put_object(test_bucket, key, b"exists")
        assert client.object_exists(test_bucket, key)
