#!/usr/bin/env python3
"""Example: Generate presigned URLs with Buckerio."""

from buckerio import Buckerio


def main() -> None:
    """Demonstrate presigned URL generation."""
    client = Buckerio()
    bucket = "presigned-example"

    # Setup
    if not client.bucket_exists(bucket):
        client.create_bucket(bucket)

    # Upload a file
    client.put_object(bucket, "secret-file.txt", "This is private content!")

    # Generate presigned GET URL (valid for 1 hour)
    get_url = client.presign_get(bucket, "secret-file.txt", expires_in=3600)
    print("Presigned GET URL (1 hour):")
    print(f"  {get_url.url}")
    print()

    # Generate presigned GET URL (valid for 5 minutes)
    short_url = client.presign_get(bucket, "secret-file.txt", expires_in=300)
    print("Presigned GET URL (5 minutes):")
    print(f"  {short_url.url}")
    print()

    # Generate presigned PUT URL for uploads
    put_url = client.presign_put(bucket, "upload-target.txt", expires_in=3600)
    print("Presigned PUT URL (1 hour):")
    print(f"  {put_url.url}")
    print()

    # Example: Using presigned URL with requests
    print("To download using curl:")
    print(f'  curl "{get_url.url}"')
    print()

    print("To upload using curl:")
    print(f'  curl -X PUT -d "file content" "{put_url.url}"')


if __name__ == "__main__":
    main()
