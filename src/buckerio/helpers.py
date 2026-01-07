"""Utility functions for Buckerio SDK."""

import hashlib
import re
from typing import Union
from urllib.parse import quote


def md5_hash(data: Union[str, bytes]) -> str:
    """Calculate MD5 hash of data and return as hex string."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.md5(data).hexdigest()


def sha256_hash(data: Union[str, bytes]) -> str:
    """Calculate SHA256 hash of data and return as hex string."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def is_valid_bucket_name(name: str) -> tuple[bool, str]:
    """
    Validate bucket name according to S3 naming rules.

    Returns (is_valid, error_message).
    """
    if len(name) < 3:
        return False, "Bucket name must be at least 3 characters"
    if len(name) > 63:
        return False, "Bucket name must be at most 63 characters"
    if not re.match(r"^[a-z0-9]", name):
        return False, "Bucket name must start with a lowercase letter or number"
    if not re.match(r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$", name):
        return False, "Bucket name must end with a lowercase letter or number"
    if ".." in name:
        return False, "Bucket name cannot contain consecutive periods"
    if not re.match(r"^[a-z0-9.-]+$", name):
        return False, "Bucket name can only contain lowercase letters, numbers, hyphens, and periods"
    # Check for IP address format
    if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", name):
        return False, "Bucket name cannot be formatted as an IP address"
    return True, ""


def normalize_key(key: str) -> str:
    """Normalize object key by removing leading slashes."""
    return key.lstrip("/")


def url_encode_key(key: str) -> str:
    """URL encode an object key, preserving forward slashes."""
    return quote(key, safe="/")


def parse_etag(etag: str) -> str:
    """Remove surrounding quotes from ETag if present."""
    if etag.startswith('"') and etag.endswith('"'):
        return etag[1:-1]
    return etag


def format_size(size: int) -> str:
    """Format byte size to human readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}" if unit != "B" else f"{size} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


def content_type_from_key(key: str) -> str:
    """Guess content type from object key extension."""
    import mimetypes

    content_type, _ = mimetypes.guess_type(key)
    return content_type or "application/octet-stream"
