"""Custom exceptions for Buckerio SDK."""

from typing import Optional


class BuckerioError(Exception):
    """Base exception for all Buckerio errors."""

    def __init__(self, message: str, code: Optional[str] = None) -> None:
        super().__init__(message)
        self.message = message
        self.code = code

    def __str__(self) -> str:
        if self.code:
            return f"[{self.code}] {self.message}"
        return self.message


class BucketNotFoundError(BuckerioError):
    """Raised when a bucket does not exist."""

    def __init__(self, bucket: str) -> None:
        super().__init__(f"Bucket '{bucket}' not found", "NoSuchBucket")
        self.bucket = bucket


class BucketAlreadyExistsError(BuckerioError):
    """Raised when trying to create a bucket that already exists."""

    def __init__(self, bucket: str) -> None:
        super().__init__(f"Bucket '{bucket}' already exists", "BucketAlreadyExists")
        self.bucket = bucket


class BucketNotEmptyError(BuckerioError):
    """Raised when trying to delete a non-empty bucket."""

    def __init__(self, bucket: str) -> None:
        super().__init__(f"Bucket '{bucket}' is not empty", "BucketNotEmpty")
        self.bucket = bucket


class ObjectNotFoundError(BuckerioError):
    """Raised when an object does not exist."""

    def __init__(self, bucket: str, key: str) -> None:
        super().__init__(f"Object '{key}' not found in bucket '{bucket}'", "NoSuchKey")
        self.bucket = bucket
        self.key = key


class AccessDeniedError(BuckerioError):
    """Raised when access is denied."""

    def __init__(self, message: str = "Access denied") -> None:
        super().__init__(message, "AccessDenied")


class InvalidCredentialsError(BuckerioError):
    """Raised when credentials are invalid."""

    def __init__(self) -> None:
        super().__init__("Invalid access key or secret key", "InvalidAccessKeyId")


class ConnectionError(BuckerioError):
    """Raised when connection to server fails."""

    def __init__(self, message: str) -> None:
        super().__init__(f"Connection failed: {message}", "ConnectionError")


class InvalidBucketNameError(BuckerioError):
    """Raised when bucket name is invalid."""

    def __init__(self, bucket: str, reason: str) -> None:
        super().__init__(f"Invalid bucket name '{bucket}': {reason}", "InvalidBucketName")
        self.bucket = bucket


class ServerError(BuckerioError):
    """Raised when server returns an error."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"Server error ({status_code}): {message}", "InternalError")
        self.status_code = status_code
