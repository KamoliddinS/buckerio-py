#!/usr/bin/env python3
"""Example: Download files from Buckerio."""

import sys
from pathlib import Path
from buckerio import Buckerio, ObjectNotFoundError


def download_file(bucket: str, key: str, output_path: str) -> None:
    """Download a file from Buckerio."""
    client = Buckerio()

    try:
        print(f"Downloading s3://{bucket}/{key} to {output_path}")
        client.download_file(bucket, key, output_path)
        print("Download complete!")
    except ObjectNotFoundError:
        print(f"Error: Object not found: s3://{bucket}/{key}")
        sys.exit(1)


def download_to_memory(bucket: str, key: str) -> bytes:
    """Download a file to memory."""
    client = Buckerio()

    result = client.get_object(bucket, key)
    return result.content


def download_bucket(bucket: str, output_dir: str, prefix: str = "") -> None:
    """Download all objects from a bucket."""
    client = Buckerio()
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    for obj in client.list_all_objects(bucket, prefix=prefix):
        # Create local path
        local_path = output_path / obj.key
        local_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"Downloading {obj.key} ({obj.size} bytes)")
        client.download_file(bucket, obj.key, local_path)


def main() -> None:
    """Example usage."""
    client = Buckerio()
    bucket = "download-example"

    # Setup: create bucket and upload some files
    if not client.bucket_exists(bucket):
        client.create_bucket(bucket)

    client.put_object(bucket, "test.txt", "Hello from Buckerio!")
    client.put_object(bucket, "data/file1.json", '{"key": "value"}')
    client.put_object(bucket, "data/file2.json", '{"another": "object"}')

    # Download to memory
    content = download_to_memory(bucket, "test.txt")
    print(f"Downloaded content: {content.decode()}")

    # Download to file
    download_file(bucket, "test.txt", "/tmp/downloaded-test.txt")

    # Download entire bucket
    download_bucket(bucket, "/tmp/buckerio-download", prefix="data/")

    print("\nFiles downloaded to /tmp/buckerio-download/")


if __name__ == "__main__":
    main()
