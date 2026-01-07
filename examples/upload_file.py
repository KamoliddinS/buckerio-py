#!/usr/bin/env python3
"""Example: Upload files to Buckerio."""

import sys
from pathlib import Path
from buckerio import Buckerio


def upload_file(file_path: str, bucket: str, key: str | None = None) -> None:
    """Upload a file to Buckerio."""
    client = Buckerio()
    path = Path(file_path)

    if not path.exists():
        print(f"Error: File not found: {file_path}")
        sys.exit(1)

    # Use filename as key if not provided
    if key is None:
        key = path.name

    # Ensure bucket exists
    if not client.bucket_exists(bucket):
        print(f"Creating bucket: {bucket}")
        client.create_bucket(bucket)

    # Upload
    print(f"Uploading {path} to s3://{bucket}/{key}")
    result = client.upload_file(bucket, key, path)
    print(f"Upload complete! ETag: {result.etag}")


def upload_directory(dir_path: str, bucket: str, prefix: str = "") -> None:
    """Upload all files in a directory."""
    client = Buckerio()
    path = Path(dir_path)

    if not path.is_dir():
        print(f"Error: Not a directory: {dir_path}")
        sys.exit(1)

    # Ensure bucket exists
    if not client.bucket_exists(bucket):
        print(f"Creating bucket: {bucket}")
        client.create_bucket(bucket)

    # Upload all files
    for file_path in path.rglob("*"):
        if file_path.is_file():
            # Calculate relative key
            relative = file_path.relative_to(path)
            key = f"{prefix}/{relative}" if prefix else str(relative)

            print(f"Uploading {file_path} -> s3://{bucket}/{key}")
            result = client.upload_file(bucket, key, file_path)
            print(f"  ETag: {result.etag}")


def main() -> None:
    """Example usage."""
    # Upload a single file
    # upload_file("./README.md", "my-bucket", "docs/readme.md")

    # Upload a directory
    # upload_directory("./data", "my-bucket", "uploads")

    # Simple demo
    client = Buckerio()
    bucket = "upload-example"

    # Create bucket
    if not client.bucket_exists(bucket):
        client.create_bucket(bucket)

    # Upload from memory
    content = b"This is uploaded content from Python!"
    result = client.put_object(bucket, "memory-upload.txt", content)
    print(f"Memory upload ETag: {result.etag}")

    # Upload with metadata
    result = client.put_object(
        bucket,
        "with-metadata.txt",
        "Content with metadata",
        content_type="text/plain",
        metadata={"author": "python-sdk", "version": "1.0"},
    )
    print(f"Metadata upload ETag: {result.etag}")

    # Verify metadata
    info = client.head_object(bucket, "with-metadata.txt")
    print(f"Object metadata: {info.metadata}")


if __name__ == "__main__":
    main()
