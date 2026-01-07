"""Main Buckerio client implementation."""

import os
from pathlib import Path
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Union

from .api import S3Api
from .exceptions import InvalidBucketNameError
from .helpers import content_type_from_key, is_valid_bucket_name, normalize_key, parse_etag
from .models import (
    Bucket,
    CopyObjectResult,
    DeleteObjectResult,
    GetObjectResult,
    ListObjectsResult,
    Object,
    ObjectInfo,
    PresignedUrlResult,
    PutObjectResult,
)
from .xml_parser import parse_copy_object, parse_list_buckets, parse_list_objects_v2


class Buckerio:
    """
    High-level Buckerio client for S3-compatible object storage.

    Example:
        ```python
        from buckerio import Buckerio

        client = Buckerio(
            endpoint="http://localhost:9000",
            access_key="admin",
            secret_key="admin",
        )

        # Create a bucket
        client.create_bucket("my-bucket")

        # Upload a file
        client.upload_file("my-bucket", "hello.txt", "/path/to/file.txt")

        # Download a file
        client.download_file("my-bucket", "hello.txt", "/path/to/output.txt")
        ```
    """

    def __init__(
        self,
        endpoint: str = "http://localhost:9000",
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = "us-east-1",
        timeout: float = 30.0,
        verify_ssl: bool = True,
    ) -> None:
        """
        Initialize Buckerio client.

        Args:
            endpoint: S3 endpoint URL
            access_key: Access key (or BUCKERIO_ACCESS_KEY env var)
            secret_key: Secret key (or BUCKERIO_SECRET_KEY env var)
            region: AWS region for signing
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificates
        """
        # Get credentials from env vars if not provided
        if access_key is None:
            access_key = os.environ.get("BUCKERIO_ACCESS_KEY", "admin")
        if secret_key is None:
            secret_key = os.environ.get("BUCKERIO_SECRET_KEY", "admin")

        self._api = S3Api(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            timeout=timeout,
            verify_ssl=verify_ssl,
        )

    def close(self) -> None:
        """Close the client and release resources."""
        self._api.close()

    def __enter__(self) -> "Buckerio":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # =========================================================================
    # Bucket Operations
    # =========================================================================

    def list_buckets(self) -> List[Bucket]:
        """
        List all buckets.

        Returns:
            List of Bucket objects
        """
        response = self._api.list_buckets()
        return parse_list_buckets(response.content)

    def create_bucket(self, bucket: str) -> None:
        """
        Create a new bucket.

        Args:
            bucket: Bucket name

        Raises:
            InvalidBucketNameError: If bucket name is invalid
            BucketAlreadyExistsError: If bucket already exists
        """
        valid, reason = is_valid_bucket_name(bucket)
        if not valid:
            raise InvalidBucketNameError(bucket, reason)

        self._api.create_bucket(bucket)

    def delete_bucket(self, bucket: str) -> None:
        """
        Delete a bucket.

        Args:
            bucket: Bucket name

        Raises:
            BucketNotFoundError: If bucket doesn't exist
            BucketNotEmptyError: If bucket is not empty
        """
        self._api.delete_bucket(bucket)

    def bucket_exists(self, bucket: str) -> bool:
        """
        Check if a bucket exists.

        Args:
            bucket: Bucket name

        Returns:
            True if bucket exists, False otherwise
        """
        try:
            self._api.head_bucket(bucket)
            return True
        except Exception:
            return False

    # =========================================================================
    # Object Operations
    # =========================================================================

    def put_object(
        self,
        bucket: str,
        key: str,
        data: Union[bytes, str],
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> PutObjectResult:
        """
        Upload an object.

        Args:
            bucket: Bucket name
            key: Object key
            data: Object data (bytes or string)
            content_type: MIME type (auto-detected if not provided)
            metadata: Custom metadata dict

        Returns:
            PutObjectResult with ETag
        """
        key = normalize_key(key)
        if content_type is None:
            content_type = content_type_from_key(key)

        response = self._api.put_object(
            bucket=bucket,
            key=key,
            body=data if isinstance(data, bytes) else data.encode("utf-8"),
            content_type=content_type,
            metadata=metadata,
        )

        etag = response.headers.get("etag", "")
        return PutObjectResult(etag=parse_etag(etag))

    def get_object(self, bucket: str, key: str) -> GetObjectResult:
        """
        Download an object.

        Args:
            bucket: Bucket name
            key: Object key

        Returns:
            GetObjectResult with content and metadata
        """
        key = normalize_key(key)
        response = self._api.get_object(bucket, key)

        # Parse headers
        etag = response.headers.get("etag")
        if etag:
            etag = parse_etag(etag)

        content_type = response.headers.get("content-type")
        content_length = int(response.headers.get("content-length", 0))

        # Parse custom metadata
        metadata = {}
        for header, value in response.headers.items():
            if header.lower().startswith("x-amz-meta-"):
                meta_key = header[11:]  # Remove "x-amz-meta-" prefix
                metadata[meta_key] = value

        return GetObjectResult(
            content=response.content,
            etag=etag,
            content_type=content_type,
            content_length=content_length,
            metadata=metadata,
        )

    def delete_object(self, bucket: str, key: str) -> DeleteObjectResult:
        """
        Delete an object.

        Args:
            bucket: Bucket name
            key: Object key

        Returns:
            DeleteObjectResult
        """
        key = normalize_key(key)
        self._api.delete_object(bucket, key)
        return DeleteObjectResult(deleted=True)

    def head_object(self, bucket: str, key: str) -> ObjectInfo:
        """
        Get object metadata without downloading content.

        Args:
            bucket: Bucket name
            key: Object key

        Returns:
            ObjectInfo with metadata
        """
        key = normalize_key(key)
        response = self._api.head_object(bucket, key)

        etag = response.headers.get("etag")
        if etag:
            etag = parse_etag(etag)

        # Parse custom metadata
        metadata = {}
        for header, value in response.headers.items():
            if header.lower().startswith("x-amz-meta-"):
                meta_key = header[11:]
                metadata[meta_key] = value

        return ObjectInfo(
            key=key,
            size=int(response.headers.get("content-length", 0)),
            etag=etag,
            content_type=response.headers.get("content-type"),
            metadata=metadata,
        )

    def object_exists(self, bucket: str, key: str) -> bool:
        """
        Check if an object exists.

        Args:
            bucket: Bucket name
            key: Object key

        Returns:
            True if object exists, False otherwise
        """
        try:
            self._api.head_object(bucket, normalize_key(key))
            return True
        except Exception:
            return False

    def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        dest_bucket: str,
        dest_key: str,
    ) -> CopyObjectResult:
        """
        Copy an object.

        Args:
            source_bucket: Source bucket name
            source_key: Source object key
            dest_bucket: Destination bucket name
            dest_key: Destination object key

        Returns:
            CopyObjectResult with ETag and timestamp
        """
        response = self._api.copy_object(
            source_bucket=source_bucket,
            source_key=normalize_key(source_key),
            dest_bucket=dest_bucket,
            dest_key=normalize_key(dest_key),
        )

        etag, last_modified = parse_copy_object(response.content)
        return CopyObjectResult(etag=etag, last_modified=last_modified)

    # =========================================================================
    # Listing Operations
    # =========================================================================

    def list_objects(
        self,
        bucket: str,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        max_keys: int = 1000,
        continuation_token: Optional[str] = None,
    ) -> ListObjectsResult:
        """
        List objects in a bucket.

        Args:
            bucket: Bucket name
            prefix: Filter by key prefix
            delimiter: Delimiter for hierarchical listing
            max_keys: Maximum objects to return
            continuation_token: Token for pagination

        Returns:
            ListObjectsResult with objects and pagination info
        """
        response = self._api.list_objects_v2(
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
            continuation_token=continuation_token,
        )
        return parse_list_objects_v2(response.content)

    def list_all_objects(
        self,
        bucket: str,
        prefix: Optional[str] = None,
    ) -> Iterator[Object]:
        """
        Iterate over all objects in a bucket (handles pagination).

        Args:
            bucket: Bucket name
            prefix: Filter by key prefix

        Yields:
            Object instances
        """
        continuation_token: Optional[str] = None

        while True:
            result = self.list_objects(
                bucket=bucket,
                prefix=prefix,
                continuation_token=continuation_token,
            )

            yield from result.objects

            if not result.is_truncated:
                break

            continuation_token = result.next_continuation_token

    # =========================================================================
    # File Operations
    # =========================================================================

    def upload_file(
        self,
        bucket: str,
        key: str,
        file_path: Union[str, Path],
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> PutObjectResult:
        """
        Upload a file from disk.

        Args:
            bucket: Bucket name
            key: Object key
            file_path: Path to local file
            content_type: MIME type (auto-detected from filename if not provided)
            metadata: Custom metadata dict

        Returns:
            PutObjectResult with ETag
        """
        file_path = Path(file_path)
        if content_type is None:
            content_type = content_type_from_key(str(file_path))

        with open(file_path, "rb") as f:
            data = f.read()

        return self.put_object(
            bucket=bucket,
            key=key,
            data=data,
            content_type=content_type,
            metadata=metadata,
        )

    def upload_fileobj(
        self,
        bucket: str,
        key: str,
        fileobj: BinaryIO,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> PutObjectResult:
        """
        Upload from a file-like object.

        Args:
            bucket: Bucket name
            key: Object key
            fileobj: File-like object with read() method
            content_type: MIME type
            metadata: Custom metadata dict

        Returns:
            PutObjectResult with ETag
        """
        data = fileobj.read()
        return self.put_object(
            bucket=bucket,
            key=key,
            data=data,
            content_type=content_type,
            metadata=metadata,
        )

    def download_file(
        self,
        bucket: str,
        key: str,
        file_path: Union[str, Path],
    ) -> None:
        """
        Download an object to a file.

        Args:
            bucket: Bucket name
            key: Object key
            file_path: Path to save file
        """
        result = self.get_object(bucket, key)
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(result.content)

    def download_fileobj(
        self,
        bucket: str,
        key: str,
        fileobj: BinaryIO,
    ) -> None:
        """
        Download an object to a file-like object.

        Args:
            bucket: Bucket name
            key: Object key
            fileobj: File-like object with write() method
        """
        result = self.get_object(bucket, key)
        fileobj.write(result.content)

    # =========================================================================
    # Presigned URLs
    # =========================================================================

    def presign_get(
        self,
        bucket: str,
        key: str,
        expires_in: int = 3600,
    ) -> PresignedUrlResult:
        """
        Generate a presigned URL for downloading an object.

        Args:
            bucket: Bucket name
            key: Object key
            expires_in: URL expiration in seconds (default 1 hour)

        Returns:
            PresignedUrlResult with URL
        """
        url = self._api.presign_get(bucket, normalize_key(key), expires_in)
        return PresignedUrlResult(url=url, expires_in=expires_in)

    def presign_put(
        self,
        bucket: str,
        key: str,
        expires_in: int = 3600,
    ) -> PresignedUrlResult:
        """
        Generate a presigned URL for uploading an object.

        Args:
            bucket: Bucket name
            key: Object key
            expires_in: URL expiration in seconds (default 1 hour)

        Returns:
            PresignedUrlResult with URL
        """
        url = self._api.presign_put(bucket, normalize_key(key), expires_in)
        return PresignedUrlResult(url=url, expires_in=expires_in)
