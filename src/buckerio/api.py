"""Low-level S3 API calls."""

from typing import Any, Dict, Optional, Union
from urllib.parse import urljoin

import httpx

from .auth import AWSV4Auth
from .exceptions import (
    AccessDeniedError,
    BucketAlreadyExistsError,
    BucketNotEmptyError,
    BucketNotFoundError,
    BuckerioError,
    ConnectionError,
    InvalidCredentialsError,
    ObjectNotFoundError,
    ServerError,
)
from .xml_parser import parse_error


class S3Api:
    """
    Low-level S3 API client.

    Handles HTTP requests, authentication, and error handling.
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
        timeout: float = 30.0,
        verify_ssl: bool = True,
    ) -> None:
        """
        Initialize the S3 API client.

        Args:
            endpoint: S3 endpoint URL (e.g., http://localhost:9000)
            access_key: AWS access key ID
            secret_key: AWS secret access key
            region: AWS region (default: us-east-1)
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificates
        """
        self.endpoint = endpoint.rstrip("/")
        self.auth = AWSV4Auth(access_key, secret_key, region)
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self._client: Optional[httpx.Client] = None

    @property
    def client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                timeout=self.timeout,
                verify=self.verify_ssl,
                follow_redirects=True,
            )
        return self._client

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self) -> "S3Api":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _build_url(self, bucket: Optional[str] = None, key: Optional[str] = None) -> str:
        """Build URL for S3 request."""
        if bucket and key:
            return f"{self.endpoint}/{bucket}/{key.lstrip('/')}"
        elif bucket:
            return f"{self.endpoint}/{bucket}"
        return self.endpoint

    def _get_host(self) -> str:
        """Extract host from endpoint URL."""
        from urllib.parse import urlparse

        parsed = urlparse(self.endpoint)
        return parsed.netloc

    def _handle_error(
        self,
        response: httpx.Response,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
    ) -> None:
        """Handle error responses."""
        status = response.status_code

        # Try to parse XML error
        try:
            code, message = parse_error(response.content)
        except Exception:
            code = "UnknownError"
            message = response.text or f"HTTP {status}"

        # Map to specific exceptions
        if status == 404:
            if code == "NoSuchBucket" or (bucket and not key):
                raise BucketNotFoundError(bucket or "unknown")
            if code == "NoSuchKey" or key:
                raise ObjectNotFoundError(bucket or "unknown", key or "unknown")

        if status == 403:
            if code == "AccessDenied":
                raise AccessDeniedError(message)
            raise InvalidCredentialsError()

        if status == 409:
            if code == "BucketAlreadyExists" or code == "BucketAlreadyOwnedByYou":
                raise BucketAlreadyExistsError(bucket or "unknown")
            if code == "BucketNotEmpty":
                raise BucketNotEmptyError(bucket or "unknown")

        if status >= 500:
            raise ServerError(status, message)

        raise BuckerioError(message, code)

    def request(
        self,
        method: str,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
        body: bytes = b"",
        headers: Optional[Dict[str, str]] = None,
        query_params: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        """
        Make a signed S3 request.

        Args:
            method: HTTP method
            bucket: Bucket name (optional)
            key: Object key (optional)
            body: Request body
            headers: Additional headers
            query_params: Query parameters

        Returns:
            HTTP response

        Raises:
            Various BuckerioError subclasses on error
        """
        url = self._build_url(bucket, key)

        # Build headers
        request_headers = {"host": self._get_host()}
        if headers:
            request_headers.update(headers)

        # Sign request
        signed_headers = self.auth.sign_request(
            method=method,
            url=url,
            headers=request_headers,
            body=body,
            query_params=query_params,
        )

        try:
            response = self.client.request(
                method=method,
                url=url,
                headers=signed_headers,
                content=body,
                params=query_params,
            )
        except httpx.ConnectError as e:
            raise ConnectionError(str(e))
        except httpx.TimeoutException as e:
            raise ConnectionError(f"Request timed out: {e}")

        # Check for errors
        if response.status_code >= 400:
            self._handle_error(response, bucket, key)

        return response

    # Bucket operations

    def list_buckets(self) -> httpx.Response:
        """List all buckets."""
        return self.request("GET")

    def create_bucket(self, bucket: str) -> httpx.Response:
        """Create a bucket."""
        return self.request("PUT", bucket=bucket)

    def delete_bucket(self, bucket: str) -> httpx.Response:
        """Delete a bucket."""
        return self.request("DELETE", bucket=bucket)

    def head_bucket(self, bucket: str) -> httpx.Response:
        """Check if bucket exists."""
        return self.request("HEAD", bucket=bucket)

    # Object operations

    def put_object(
        self,
        bucket: str,
        key: str,
        body: Union[bytes, str],
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        """Upload an object."""
        if isinstance(body, str):
            body = body.encode("utf-8")

        headers: Dict[str, str] = {}
        if content_type:
            headers["content-type"] = content_type
        if metadata:
            for k, v in metadata.items():
                headers[f"x-amz-meta-{k}"] = v

        return self.request("PUT", bucket=bucket, key=key, body=body, headers=headers)

    def get_object(self, bucket: str, key: str) -> httpx.Response:
        """Download an object."""
        return self.request("GET", bucket=bucket, key=key)

    def delete_object(self, bucket: str, key: str) -> httpx.Response:
        """Delete an object."""
        return self.request("DELETE", bucket=bucket, key=key)

    def head_object(self, bucket: str, key: str) -> httpx.Response:
        """Get object metadata."""
        return self.request("HEAD", bucket=bucket, key=key)

    def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        dest_bucket: str,
        dest_key: str,
    ) -> httpx.Response:
        """Copy an object."""
        headers = {"x-amz-copy-source": f"/{source_bucket}/{source_key}"}
        return self.request("PUT", bucket=dest_bucket, key=dest_key, headers=headers)

    def list_objects_v2(
        self,
        bucket: str,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        max_keys: int = 1000,
        continuation_token: Optional[str] = None,
    ) -> httpx.Response:
        """List objects in a bucket using V2 API."""
        params: Dict[str, str] = {"list-type": "2"}
        if prefix:
            params["prefix"] = prefix
        if delimiter:
            params["delimiter"] = delimiter
        if max_keys != 1000:
            params["max-keys"] = str(max_keys)
        if continuation_token:
            params["continuation-token"] = continuation_token

        return self.request("GET", bucket=bucket, query_params=params)

    # Presigned URLs

    def presign_get(self, bucket: str, key: str, expires_in: int = 3600) -> str:
        """Generate presigned URL for GET."""
        url = self._build_url(bucket, key)
        return self.auth.presign_url("GET", url, expires_in)

    def presign_put(self, bucket: str, key: str, expires_in: int = 3600) -> str:
        """Generate presigned URL for PUT."""
        url = self._build_url(bucket, key)
        return self.auth.presign_url("PUT", url, expires_in)
