"""Tests for presigned URL operations."""

from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import pytest
import requests

from buckerio import Buckerio
from buckerio.auth import AWSV4Auth


class TestPresignedUrlGeneration:
    """Pure unit tests for presigned URL generation (no server required)."""

    def test_presign_url_basic(self) -> None:
        """Test basic presign_url generation."""
        auth = AWSV4Auth("test-access-key", "test-secret-key", "us-east-1")

        url = auth.presign_url(
            method="GET",
            url="http://localhost:9000/my-bucket/my-key",
            expires_in=3600,
        )

        assert "X-Amz-Signature" in url
        assert "X-Amz-Credential" in url
        assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in url
        assert "X-Amz-Expires=3600" in url

    def test_presign_url_with_query_params(self) -> None:
        """Test presign_url with additional query parameters."""
        auth = AWSV4Auth("access-key", "secret-key", "us-east-1")

        url = auth.presign_url(
            method="GET",
            url="http://localhost:9000/bucket/key",
            expires_in=7200,
            query_params={
                "response-content-disposition": "attachment; filename=test.txt",
                "versionId": "v123",
            },
        )

        assert "response-content-disposition" in url
        assert "versionId=v123" in url
        assert "X-Amz-Expires=7200" in url

    def test_presign_url_with_request_date(self) -> None:
        """Test presign_url with custom request date."""
        auth = AWSV4Auth("access-key", "secret-key", "us-east-1")

        fixed_date = datetime(2024, 6, 15, 12, 30, 45, tzinfo=timezone.utc)
        url = auth.presign_url(
            method="GET",
            url="http://localhost:9000/bucket/key",
            expires_in=3600,
            request_date=fixed_date,
        )

        # Check the date is in the URL
        assert "X-Amz-Date=20240615T123045Z" in url
        # Credential should include the date stamp
        assert "20240615" in url

    def test_presign_url_reproducibility(self) -> None:
        """Test that same inputs produce same URL."""
        auth = AWSV4Auth("access-key", "secret-key", "us-east-1")
        fixed_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        url1 = auth.presign_url(
            method="GET",
            url="http://localhost:9000/bucket/key",
            expires_in=3600,
            request_date=fixed_date,
        )
        url2 = auth.presign_url(
            method="GET",
            url="http://localhost:9000/bucket/key",
            expires_in=3600,
            request_date=fixed_date,
        )

        assert url1 == url2


class TestS3ApiPresignGetObject:
    """Unit tests for S3Api.presign_get_object method."""

    def test_presign_get_object_basic(self) -> None:
        """Test basic presign_get_object without server."""
        from buckerio.api import S3Api

        api = S3Api(
            endpoint="http://localhost:9000",
            access_key="test-access",
            secret_key="test-secret",
        )

        url = api.presign_get_object("my-bucket", "my-key")

        assert "my-bucket" in url
        assert "my-key" in url
        assert "X-Amz-Signature" in url
        # Default is 7 days
        assert "X-Amz-Expires=604800" in url

    def test_presign_get_object_with_response_headers(self) -> None:
        """Test presign_get_object with response headers."""
        from buckerio.api import S3Api

        api = S3Api(
            endpoint="http://localhost:9000",
            access_key="access",
            secret_key="secret",
        )

        url = api.presign_get_object(
            bucket="bucket",
            key="file.pdf",
            response_headers={
                "content-type": "application/pdf",
                "content-disposition": "attachment; filename=report.pdf",
            },
        )

        assert "response-content-type=application" in url
        assert "response-content-disposition=attachment" in url

    def test_presign_get_object_with_version_id(self) -> None:
        """Test presign_get_object with version ID."""
        from buckerio.api import S3Api

        api = S3Api(
            endpoint="http://localhost:9000",
            access_key="access",
            secret_key="secret",
        )

        url = api.presign_get_object(
            bucket="bucket", key="key", version_id="abc123"
        )

        assert "versionId=abc123" in url

    def test_presign_get_object_with_extra_params(self) -> None:
        """Test presign_get_object with extra query params."""
        from buckerio.api import S3Api

        api = S3Api(
            endpoint="http://localhost:9000",
            access_key="access",
            secret_key="secret",
        )

        url = api.presign_get_object(
            bucket="bucket",
            key="key",
            extra_query_params={"custom": "value"},
        )

        assert "custom=value" in url


class TestBuckerioClientPresignedGetObject:
    """Unit tests for Buckerio.presigned_get_object method."""

    def test_presigned_get_object_basic(self) -> None:
        """Test Buckerio.presigned_get_object without server."""
        client = Buckerio(
            endpoint="http://localhost:9000",
            access_key="test-access",
            secret_key="test-secret",
        )

        result = client.presigned_get_object("my-bucket", "my-object.txt")

        assert result.url is not None
        assert "my-bucket" in result.url
        assert "my-object.txt" in result.url
        assert "X-Amz-Signature" in result.url
        assert result.expires_in == 604800  # 7 days default

    def test_presigned_get_object_custom_expiry(self) -> None:
        """Test presigned_get_object with custom expiration."""
        client = Buckerio(
            endpoint="http://localhost:9000",
            access_key="access",
            secret_key="secret",
        )

        result = client.presigned_get_object(
            "bucket", "key", expires=timedelta(hours=2)
        )

        assert result.expires_in == 7200  # 2 hours in seconds
        assert "X-Amz-Expires=7200" in result.url

    def test_presigned_get_object_all_options(self) -> None:
        """Test presigned_get_object with all options."""
        client = Buckerio(
            endpoint="http://localhost:9000",
            access_key="access",
            secret_key="secret",
        )

        fixed_date = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        result = client.presigned_get_object(
            bucket_name="test-bucket",
            object_name="path/to/file.pdf",
            expires=timedelta(days=1),
            response_headers={
                "content-disposition": "attachment; filename=download.pdf",
                "content-type": "application/pdf",
            },
            version_id="v1",
            extra_query_params={"custom": "param"},
            request_date=fixed_date,
        )

        assert result.expires_in == 86400  # 1 day
        assert "20240615T120000Z" in result.url
        assert "response-content-disposition" in result.url
        assert "response-content-type" in result.url
        assert "versionId=v1" in result.url
        assert "custom=param" in result.url


@pytest.mark.integration
class TestPresignedUrls:
    """Integration tests for presigned URL generation (requires running server)."""

    def test_presigned_get_object_basic(self, client: Buckerio, test_bucket: str) -> None:
        """Test basic presigned_get_object URL generation."""
        # Create an object first
        client.put_object(test_bucket, "test-presigned.txt", b"Hello World!")

        # Generate presigned URL
        result = client.presigned_get_object(test_bucket, "test-presigned.txt")

        # Verify result
        assert result.url is not None
        assert "X-Amz-Signature" in result.url
        assert "X-Amz-Credential" in result.url
        assert "X-Amz-Algorithm" in result.url
        assert result.expires_in == 604800  # 7 days default

    def test_presigned_get_object_custom_expiry(
        self, client: Buckerio, test_bucket: str
    ) -> None:
        """Test presigned URL with custom expiration."""
        client.put_object(test_bucket, "test-expiry.txt", b"Content")

        result = client.presigned_get_object(
            test_bucket, "test-expiry.txt", expires=timedelta(hours=2)
        )

        assert result.expires_in == 7200  # 2 hours in seconds
        # Check X-Amz-Expires in URL
        parsed = urlparse(result.url)
        query = parse_qs(parsed.query)
        assert query["X-Amz-Expires"][0] == "7200"

    def test_presigned_get_object_with_content_disposition(
        self, client: Buckerio, test_bucket: str
    ) -> None:
        """Test presigned URL with response-content-disposition."""
        client.put_object(test_bucket, "report.pdf", b"PDF content")

        result = client.presigned_get_object(
            test_bucket,
            "report.pdf",
            response_headers={
                "content-disposition": "attachment; filename=Q4-Report.pdf"
            },
        )

        # Verify response-content-disposition is in URL
        assert "response-content-disposition" in result.url
        assert "attachment" in result.url

    def test_presigned_get_object_with_multiple_headers(
        self, client: Buckerio, test_bucket: str
    ) -> None:
        """Test presigned URL with multiple response headers."""
        client.put_object(test_bucket, "data.json", b'{"key": "value"}')

        result = client.presigned_get_object(
            test_bucket,
            "data.json",
            response_headers={
                "content-type": "application/json",
                "content-disposition": "inline",
                "cache-control": "no-cache",
            },
        )

        assert "response-content-type" in result.url
        assert "response-content-disposition" in result.url
        assert "response-cache-control" in result.url

    def test_presigned_get_object_with_version_id(
        self, client: Buckerio, test_bucket: str
    ) -> None:
        """Test presigned URL with version ID parameter."""
        client.put_object(test_bucket, "versioned.txt", b"Version 1")

        result = client.presigned_get_object(
            test_bucket, "versioned.txt", version_id="test-version-123"
        )

        assert "versionId=test-version-123" in result.url

    def test_presigned_get_object_with_extra_params(
        self, client: Buckerio, test_bucket: str
    ) -> None:
        """Test presigned URL with extra query parameters."""
        client.put_object(test_bucket, "extra.txt", b"Extra params test")

        result = client.presigned_get_object(
            test_bucket,
            "extra.txt",
            extra_query_params={"custom-param": "custom-value"},
        )

        assert "custom-param=custom-value" in result.url

    def test_presigned_get_object_with_request_date(
        self, client: Buckerio, test_bucket: str
    ) -> None:
        """Test presigned URL with custom request date for reproducibility."""
        client.put_object(test_bucket, "dated.txt", b"Dated content")

        # Use a fixed date
        fixed_date = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        result = client.presigned_get_object(
            test_bucket, "dated.txt", request_date=fixed_date
        )

        # The URL should contain the fixed date
        assert "20240615T120000Z" in result.url

    def test_presigned_get_object_url_structure(
        self, client: Buckerio, test_bucket: str
    ) -> None:
        """Test that presigned URL has correct structure."""
        client.put_object(test_bucket, "structure-test.txt", b"Test")

        result = client.presigned_get_object(test_bucket, "structure-test.txt")

        parsed = urlparse(result.url)

        # Verify path includes bucket and key
        assert test_bucket in parsed.path
        assert "structure-test.txt" in parsed.path

        # Verify required S3 signature params
        query = parse_qs(parsed.query)
        assert "X-Amz-Algorithm" in query
        assert query["X-Amz-Algorithm"][0] == "AWS4-HMAC-SHA256"
        assert "X-Amz-Credential" in query
        assert "X-Amz-Date" in query
        assert "X-Amz-Expires" in query
        assert "X-Amz-SignedHeaders" in query
        assert "X-Amz-Signature" in query


@pytest.mark.integration
class TestPresignedUrlsIntegration:
    """Integration tests for presigned URLs (requires running server)."""

    def test_presigned_url_download(self, client: Buckerio, test_bucket: str) -> None:
        """Test that presigned URL can be used to download content."""
        content = b"Integration test content!"
        client.put_object(test_bucket, "download-test.txt", content)

        # Generate presigned URL
        result = client.presigned_get_object(
            test_bucket, "download-test.txt", expires=timedelta(hours=1)
        )

        # Download using the presigned URL (no auth needed)
        response = requests.get(result.url)
        assert response.status_code == 200
        assert response.content == content

    def test_presigned_url_with_special_characters(
        self, client: Buckerio, test_bucket: str
    ) -> None:
        """Test presigned URL with special characters in key."""
        content = b"Special chars test"
        key = "folder/sub folder/file with spaces.txt"

        client.put_object(test_bucket, key, content)
        result = client.presigned_get_object(test_bucket, key)

        response = requests.get(result.url)
        assert response.status_code == 200
        assert response.content == content

    def test_old_presign_get_still_works(
        self, client: Buckerio, test_bucket: str
    ) -> None:
        """Test that the original presign_get method still works."""
        content = b"Backward compatibility test"
        client.put_object(test_bucket, "compat-test.txt", content)

        # Use the original simpler API
        result = client.presign_get(test_bucket, "compat-test.txt", expires_in=3600)

        response = requests.get(result.url)
        assert response.status_code == 200
        assert response.content == content
