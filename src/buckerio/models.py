"""Data models for Buckerio SDK."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class Bucket:
    """Represents an S3 bucket."""

    name: str
    creation_date: Optional[datetime] = None

    def __str__(self) -> str:
        return self.name


@dataclass
class Object:
    """Represents an S3 object."""

    key: str
    size: int = 0
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    storage_class: str = "STANDARD"

    def __str__(self) -> str:
        return self.key


@dataclass
class ObjectInfo:
    """Detailed object metadata from HEAD request."""

    key: str
    size: int
    etag: Optional[str] = None
    content_type: Optional[str] = None
    last_modified: Optional[datetime] = None
    metadata: dict = field(default_factory=dict)


@dataclass
class ListObjectsResult:
    """Result of listing objects in a bucket."""

    objects: List[Object]
    common_prefixes: List[str] = field(default_factory=list)
    is_truncated: bool = False
    continuation_token: Optional[str] = None
    next_continuation_token: Optional[str] = None
    prefix: Optional[str] = None
    delimiter: Optional[str] = None
    max_keys: int = 1000
    key_count: int = 0


@dataclass
class PutObjectResult:
    """Result of uploading an object."""

    etag: str
    version_id: Optional[str] = None


@dataclass
class GetObjectResult:
    """Result of downloading an object."""

    content: bytes
    etag: Optional[str] = None
    content_type: Optional[str] = None
    content_length: int = 0
    last_modified: Optional[datetime] = None
    metadata: dict = field(default_factory=dict)


@dataclass
class Owner:
    """Represents a bucket/object owner."""

    id: str
    display_name: Optional[str] = None


@dataclass
class CopyObjectResult:
    """Result of copying an object."""

    etag: str
    last_modified: Optional[datetime] = None


@dataclass
class DeleteObjectResult:
    """Result of deleting an object."""

    deleted: bool = True
    version_id: Optional[str] = None


@dataclass
class PresignedUrlResult:
    """Result containing a presigned URL."""

    url: str
    expires_in: int  # seconds
