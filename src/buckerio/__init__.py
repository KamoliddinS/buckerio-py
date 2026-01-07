"""
Buckerio Python SDK - S3-compatible object storage client.

Example:
    ```python
    from buckerio import Buckerio

    client = Buckerio(
        endpoint="http://localhost:9000",
        access_key="admin",
        secret_key="admin",
    )

    # Create bucket
    client.create_bucket("my-bucket")

    # Upload
    client.put_object("my-bucket", "hello.txt", b"Hello, World!")

    # Download
    result = client.get_object("my-bucket", "hello.txt")
    print(result.content)

    # List objects
    for obj in client.list_all_objects("my-bucket"):
        print(f"{obj.key}: {obj.size} bytes")
    ```
"""

from .client import Buckerio
from .exceptions import (
    AccessDeniedError,
    BucketAlreadyExistsError,
    BucketNotEmptyError,
    BucketNotFoundError,
    BuckerioError,
    ConnectionError,
    InvalidBucketNameError,
    InvalidCredentialsError,
    ObjectNotFoundError,
    ServerError,
)
from .models import (
    Bucket,
    CopyObjectResult,
    DeleteObjectResult,
    GetObjectResult,
    ListObjectsResult,
    Object,
    ObjectInfo,
    Owner,
    PresignedUrlResult,
    PutObjectResult,
)
from .__version__ import __version__

__all__ = [
    # Main client
    "Buckerio",
    # Models
    "Bucket",
    "Object",
    "ObjectInfo",
    "Owner",
    "ListObjectsResult",
    "PutObjectResult",
    "GetObjectResult",
    "CopyObjectResult",
    "DeleteObjectResult",
    "PresignedUrlResult",
    # Exceptions
    "BuckerioError",
    "BucketNotFoundError",
    "BucketAlreadyExistsError",
    "BucketNotEmptyError",
    "ObjectNotFoundError",
    "AccessDeniedError",
    "InvalidCredentialsError",
    "InvalidBucketNameError",
    "ConnectionError",
    "ServerError",
    # Version
    "__version__",
]
