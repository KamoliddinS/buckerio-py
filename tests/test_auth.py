"""Tests for AWS Signature V4 authentication."""

import pytest
from buckerio.auth import AWSV4Auth


class TestAWSV4Auth:
    """Test AWS Signature V4 signing."""

    def test_init(self) -> None:
        """Test auth initialization."""
        auth = AWSV4Auth(
            access_key="AKIAIOSFODNN7EXAMPLE",
            secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region="us-east-1",
        )
        assert auth.access_key == "AKIAIOSFODNN7EXAMPLE"
        assert auth.region == "us-east-1"

    def test_sign_request_adds_headers(self) -> None:
        """Test that signing adds required headers."""
        auth = AWSV4Auth(
            access_key="testkey",
            secret_key="testsecret",
            region="us-east-1",
        )

        headers = auth.sign_request(
            method="GET",
            url="http://localhost:9000/test-bucket",
            headers={"host": "localhost:9000"},
        )

        assert "Authorization" in headers
        assert "x-amz-date" in headers
        assert "x-amz-content-sha256" in headers
        assert headers["Authorization"].startswith("AWS4-HMAC-SHA256")

    def test_sign_request_with_body(self) -> None:
        """Test signing request with body."""
        auth = AWSV4Auth(
            access_key="testkey",
            secret_key="testsecret",
        )

        body = b"Hello, World!"
        headers = auth.sign_request(
            method="PUT",
            url="http://localhost:9000/bucket/key",
            headers={"host": "localhost:9000"},
            body=body,
        )

        assert "Authorization" in headers
        # Content hash should be SHA256 of body
        import hashlib
        expected_hash = hashlib.sha256(body).hexdigest()
        assert headers["x-amz-content-sha256"] == expected_hash

    def test_presign_url(self) -> None:
        """Test presigned URL generation."""
        auth = AWSV4Auth(
            access_key="testkey",
            secret_key="testsecret",
            region="us-east-1",
        )

        url = auth.presign_url(
            method="GET",
            url="http://localhost:9000/bucket/object.txt",
            expires_in=3600,
        )

        assert "http://localhost:9000/bucket/object.txt?" in url
        assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in url
        assert "X-Amz-Credential=testkey" in url
        assert "X-Amz-Expires=3600" in url
        assert "X-Amz-Signature=" in url

    def test_different_regions(self) -> None:
        """Test signing with different regions."""
        for region in ["us-east-1", "eu-west-1", "ap-southeast-1"]:
            auth = AWSV4Auth(
                access_key="testkey",
                secret_key="testsecret",
                region=region,
            )
            headers = auth.sign_request(
                method="GET",
                url="http://localhost:9000/",
                headers={"host": "localhost:9000"},
            )
            assert f"/{region}/s3/aws4_request" in headers["Authorization"]
