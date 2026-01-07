"""XML parser for S3 API responses."""

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Optional, Tuple

from .models import Bucket, ListObjectsResult, Object, Owner

# S3 XML namespace
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


def _find_text(element: ET.Element, path: str, ns: dict = S3_NS) -> Optional[str]:
    """Find text content of a child element."""
    # Try with namespace
    child = element.find(f"s3:{path}", ns)
    if child is None:
        # Try without namespace (some responses don't use it)
        child = element.find(path)
    return child.text if child is not None else None


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse ISO 8601 datetime string."""
    if not value:
        return None
    try:
        # Handle format: 2024-01-15T10:30:00.000Z
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def parse_error(xml_content: bytes) -> Tuple[str, str]:
    """
    Parse an S3 error response.

    Returns (error_code, error_message).
    """
    try:
        root = ET.fromstring(xml_content)
        code = _find_text(root, "Code") or "UnknownError"
        message = _find_text(root, "Message") or "Unknown error occurred"
        return code, message
    except ET.ParseError:
        return "ParseError", "Failed to parse error response"


def parse_list_buckets(xml_content: bytes) -> List[Bucket]:
    """Parse ListBuckets response."""
    root = ET.fromstring(xml_content)
    buckets: List[Bucket] = []

    # Find Buckets container
    buckets_elem = root.find("s3:Buckets", S3_NS)
    if buckets_elem is None:
        buckets_elem = root.find("Buckets")

    if buckets_elem is None:
        return buckets

    # Parse each bucket
    for bucket_elem in buckets_elem:
        name = _find_text(bucket_elem, "Name")
        if name:
            creation_date = _parse_datetime(_find_text(bucket_elem, "CreationDate"))
            buckets.append(Bucket(name=name, creation_date=creation_date))

    return buckets


def parse_list_objects_v2(xml_content: bytes) -> ListObjectsResult:
    """Parse ListObjectsV2 response."""
    root = ET.fromstring(xml_content)

    objects: List[Object] = []
    common_prefixes: List[str] = []

    # Parse metadata
    is_truncated = _find_text(root, "IsTruncated") == "true"
    continuation_token = _find_text(root, "ContinuationToken")
    next_token = _find_text(root, "NextContinuationToken")
    prefix = _find_text(root, "Prefix")
    delimiter = _find_text(root, "Delimiter")
    max_keys_str = _find_text(root, "MaxKeys")
    max_keys = int(max_keys_str) if max_keys_str else 1000
    key_count_str = _find_text(root, "KeyCount")
    key_count = int(key_count_str) if key_count_str else 0

    # Parse Contents (objects)
    for content_elem in root.findall("s3:Contents", S3_NS) or root.findall("Contents"):
        key = _find_text(content_elem, "Key")
        if key:
            size_str = _find_text(content_elem, "Size")
            size = int(size_str) if size_str else 0
            etag = _find_text(content_elem, "ETag")
            if etag:
                etag = etag.strip('"')
            last_modified = _parse_datetime(_find_text(content_elem, "LastModified"))
            storage_class = _find_text(content_elem, "StorageClass") or "STANDARD"

            objects.append(
                Object(
                    key=key,
                    size=size,
                    etag=etag,
                    last_modified=last_modified,
                    storage_class=storage_class,
                )
            )

    # Parse CommonPrefixes
    for prefix_elem in root.findall("s3:CommonPrefixes", S3_NS) or root.findall("CommonPrefixes"):
        prefix_value = _find_text(prefix_elem, "Prefix")
        if prefix_value:
            common_prefixes.append(prefix_value)

    return ListObjectsResult(
        objects=objects,
        common_prefixes=common_prefixes,
        is_truncated=is_truncated,
        continuation_token=continuation_token,
        next_continuation_token=next_token,
        prefix=prefix,
        delimiter=delimiter,
        max_keys=max_keys,
        key_count=key_count,
    )


def parse_copy_object(xml_content: bytes) -> Tuple[str, Optional[datetime]]:
    """
    Parse CopyObject response.

    Returns (etag, last_modified).
    """
    root = ET.fromstring(xml_content)
    etag = _find_text(root, "ETag") or ""
    if etag:
        etag = etag.strip('"')
    last_modified = _parse_datetime(_find_text(root, "LastModified"))
    return etag, last_modified


def parse_owner(xml_content: bytes) -> Optional[Owner]:
    """Parse Owner element from various responses."""
    try:
        root = ET.fromstring(xml_content)
        owner_elem = root.find("s3:Owner", S3_NS)
        if owner_elem is None:
            owner_elem = root.find("Owner")
        if owner_elem is None:
            return None

        owner_id = _find_text(owner_elem, "ID")
        display_name = _find_text(owner_elem, "DisplayName")

        if owner_id:
            return Owner(id=owner_id, display_name=display_name)
        return None
    except ET.ParseError:
        return None


def parse_create_bucket(xml_content: bytes) -> Optional[str]:
    """Parse CreateBucket response (usually empty or contains location)."""
    if not xml_content:
        return None
    try:
        root = ET.fromstring(xml_content)
        return _find_text(root, "Location")
    except ET.ParseError:
        return None
