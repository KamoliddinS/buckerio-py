# Buckerio Python SDK

A Python SDK for [Buckerio](https://github.com/KamoliddinS/buckerio) - S3-compatible object storage server.

## Installation

```bash
pip install buckerio
```

Or install from source:

```bash
git clone https://github.com/KamoliddinS/buckerio.git
cd buckerio/buckerio-py
pip install -e .
```

## Quick Start

```python
from buckerio import Buckerio

# Initialize client
client = Buckerio(
    endpoint="http://localhost:9000",
    access_key="admin",
    secret_key="admin",
)

# Create a bucket
client.create_bucket("my-bucket")

# Upload an object
client.put_object("my-bucket", "hello.txt", b"Hello, World!")

# Download an object
result = client.get_object("my-bucket", "hello.txt")
print(result.content)  # b"Hello, World!"

# List objects
for obj in client.list_all_objects("my-bucket"):
    print(f"{obj.key}: {obj.size} bytes")

# Delete an object
client.delete_object("my-bucket", "hello.txt")

# Delete the bucket
client.delete_bucket("my-bucket")
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BUCKERIO_ACCESS_KEY` | `admin` | Access key for authentication |
| `BUCKERIO_SECRET_KEY` | `admin` | Secret key for authentication |
| `BUCKERIO_ENDPOINT` | - | S3 endpoint URL (for tests) |

### Client Options

```python
client = Buckerio(
    endpoint="http://localhost:9000",  # S3 endpoint URL
    access_key="admin",                 # Access key (or env var)
    secret_key="admin",                 # Secret key (or env var)
    region="us-east-1",                 # AWS region for signing
    timeout=30.0,                       # Request timeout in seconds
    verify_ssl=True,                    # Verify SSL certificates
)
```

## API Reference

### Bucket Operations

```python
# List all buckets
buckets = client.list_buckets()
for bucket in buckets:
    print(bucket.name, bucket.creation_date)

# Create a bucket
client.create_bucket("new-bucket")

# Check if bucket exists
if client.bucket_exists("my-bucket"):
    print("Bucket exists!")

# Delete a bucket (must be empty)
client.delete_bucket("my-bucket")
```

### Object Operations

```python
# Upload object from bytes/string
result = client.put_object("bucket", "key", b"content")
print(result.etag)

# Upload with content type and metadata
result = client.put_object(
    "bucket",
    "document.json",
    '{"key": "value"}',
    content_type="application/json",
    metadata={"author": "alice", "version": "1.0"},
)

# Download object
result = client.get_object("bucket", "key")
print(result.content)       # bytes
print(result.etag)          # ETag hash
print(result.content_type)  # MIME type
print(result.metadata)      # Custom metadata

# Get object metadata (without content)
info = client.head_object("bucket", "key")
print(info.size, info.content_type)

# Check if object exists
if client.object_exists("bucket", "key"):
    print("Object exists!")

# Delete object
client.delete_object("bucket", "key")

# Copy object
client.copy_object(
    source_bucket="bucket1",
    source_key="original.txt",
    dest_bucket="bucket2",
    dest_key="copy.txt",
)
```

### File Operations

```python
# Upload from file path
client.upload_file("bucket", "remote-key.txt", "/local/path/file.txt")

# Upload from file object
with open("file.txt", "rb") as f:
    client.upload_fileobj("bucket", "key", f)

# Download to file path
client.download_file("bucket", "key", "/local/path/output.txt")

# Download to file object
with open("output.txt", "wb") as f:
    client.download_fileobj("bucket", "key", f)
```

### Listing Objects

```python
# List objects (single page)
result = client.list_objects("bucket", prefix="data/", max_keys=100)
for obj in result.objects:
    print(obj.key, obj.size)

# Handle pagination
if result.is_truncated:
    next_result = client.list_objects(
        "bucket",
        continuation_token=result.next_continuation_token,
    )

# Iterate all objects (handles pagination automatically)
for obj in client.list_all_objects("bucket", prefix="logs/"):
    print(obj.key)

# List with delimiter (folder-like structure)
result = client.list_objects("bucket", delimiter="/")
for prefix in result.common_prefixes:
    print(f"Folder: {prefix}")
```

### Presigned URLs

```python
# Generate download URL (valid for 1 hour)
result = client.presign_get("bucket", "key", expires_in=3600)
print(result.url)

# Generate upload URL
result = client.presign_put("bucket", "key", expires_in=3600)
print(result.url)

# Use with any HTTP client
import requests
response = requests.get(result.url)
```

## Error Handling

```python
from buckerio import (
    BuckerioError,
    BucketNotFoundError,
    BucketAlreadyExistsError,
    BucketNotEmptyError,
    ObjectNotFoundError,
    AccessDeniedError,
    InvalidCredentialsError,
    ConnectionError,
)

try:
    client.get_object("bucket", "nonexistent-key")
except ObjectNotFoundError as e:
    print(f"Object not found: {e.key}")
except BucketNotFoundError as e:
    print(f"Bucket not found: {e.bucket}")
except AccessDeniedError:
    print("Access denied!")
except ConnectionError as e:
    print(f"Connection failed: {e}")
except BuckerioError as e:
    print(f"Error: {e.code} - {e.message}")
```

## Context Manager

```python
# Client is automatically closed
with Buckerio() as client:
    client.put_object("bucket", "key", b"content")
```

## Development

### Install Development Dependencies

```bash
pip install -e ".[dev]"
```

### Run Tests

```bash
# Unit tests only
pytest tests/test_auth.py tests/test_client.py

# Integration tests (requires running Buckerio server)
pytest tests/ -m integration
```

### Code Quality

```bash
# Linting
ruff check src/

# Type checking
mypy src/
```

### Publishing to PyPI

```bash
# Install build tools
pip install build twine

# Build the package
python -m build

# Check the distribution
twine check dist/*

# Upload to TestPyPI (for testing)
twine upload --repository testpypi dist/*

# Upload to PyPI (production)
twine upload dist/*
```

**Prerequisites:**
- Create account on [PyPI](https://pypi.org/account/register/)
- Create API token at https://pypi.org/manage/account/token/
- Configure `~/.pypirc`:
  ```ini
  [pypi]
  username = __token__
  password = pypi-your-api-token-here
  ```

**Version bump:**
```bash
# Update version in src/buckerio/__version__.py
echo '__version__ = "0.2.0"' > src/buckerio/__version__.py

# Commit and tag
git add -A
git commit -m "chore: bump version to 0.2.0"
git tag v0.2.0
git push && git push --tags
```

## License

MIT - See [LICENSE](LICENSE) for details.
