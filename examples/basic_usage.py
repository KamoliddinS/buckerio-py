#!/usr/bin/env python3
"""Basic usage example for Buckerio SDK."""

from buckerio import Buckerio, BucketNotFoundError


def main() -> None:
    # Initialize client
    client = Buckerio(
        endpoint="http://localhost:9000",
        access_key="admin",
        secret_key="admin",
    )

    bucket_name = "example-bucket"

    # Create bucket (ignore if exists)
    try:
        client.create_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    except Exception as e:
        print(f"Bucket already exists: {e}")

    # Upload some objects
    objects = {
        "hello.txt": "Hello, World!",
        "data/file1.txt": "File 1 content",
        "data/file2.txt": "File 2 content",
    }

    for key, content in objects.items():
        result = client.put_object(bucket_name, key, content)
        print(f"Uploaded {key}, ETag: {result.etag}")

    # List all buckets
    print("\nBuckets:")
    for bucket in client.list_buckets():
        print(f"  - {bucket.name}")

    # List objects in bucket
    print(f"\nObjects in {bucket_name}:")
    for obj in client.list_all_objects(bucket_name):
        print(f"  - {obj.key} ({obj.size} bytes)")

    # Get object
    result = client.get_object(bucket_name, "hello.txt")
    print(f"\nContent of hello.txt: {result.content.decode()}")

    # Check if object exists
    exists = client.object_exists(bucket_name, "hello.txt")
    print(f"hello.txt exists: {exists}")

    # Get object metadata
    info = client.head_object(bucket_name, "hello.txt")
    print(f"hello.txt size: {info.size}, type: {info.content_type}")

    # Delete an object
    client.delete_object(bucket_name, "hello.txt")
    print("\nDeleted hello.txt")

    # Clean up - delete remaining objects and bucket
    print("\nCleaning up...")
    for obj in client.list_all_objects(bucket_name):
        client.delete_object(bucket_name, obj.key)
        print(f"  Deleted {obj.key}")

    client.delete_bucket(bucket_name)
    print(f"Deleted bucket: {bucket_name}")


if __name__ == "__main__":
    main()
