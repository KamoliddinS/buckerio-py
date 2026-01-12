"""AWS Signature Version 4 authentication for S3 API."""

import hashlib
import hmac
from datetime import datetime, timezone
from typing import Dict, Optional
from urllib.parse import quote, urlencode, urlparse


class AWSV4Auth:
    """
    AWS Signature Version 4 signer for S3 requests.

    This implements the signing process described at:
    https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
    """

    ALGORITHM = "AWS4-HMAC-SHA256"
    SERVICE = "s3"

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
    ) -> None:
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region

    def _sign(self, key: bytes, msg: str) -> bytes:
        """Create HMAC-SHA256 signature."""
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    def _get_signature_key(self, date_stamp: str) -> bytes:
        """Derive the signing key."""
        k_date = self._sign(f"AWS4{self.secret_key}".encode("utf-8"), date_stamp)
        k_region = self._sign(k_date, self.region)
        k_service = self._sign(k_region, self.SERVICE)
        k_signing = self._sign(k_service, "aws4_request")
        return k_signing

    def _get_canonical_uri(self, path: str) -> str:
        """Get canonical URI (URL-encoded path)."""
        # Split path and encode each segment
        segments = path.split("/")
        encoded_segments = [quote(s, safe="") for s in segments]
        return "/".join(encoded_segments)

    def _get_canonical_query_string(self, query_params: Dict[str, str]) -> str:
        """Get canonical query string (sorted and encoded)."""
        if not query_params:
            return ""
        # Sort by key and encode
        sorted_params = sorted(query_params.items())
        return urlencode(sorted_params, safe="-_.~")

    def _get_canonical_headers(self, headers: Dict[str, str]) -> tuple[str, str]:
        """
        Get canonical headers string and signed headers list.

        Returns (canonical_headers, signed_headers).
        """
        # Lowercase header names and trim values
        normalized = {k.lower(): v.strip() for k, v in headers.items()}
        # Sort by header name
        sorted_headers = sorted(normalized.items())
        # Build canonical headers string
        canonical_headers = "".join(f"{k}:{v}\n" for k, v in sorted_headers)
        # Build signed headers list
        signed_headers = ";".join(k for k, _ in sorted_headers)
        return canonical_headers, signed_headers

    def _sha256_hash(self, data: bytes) -> str:
        """Calculate SHA256 hash and return hex string."""
        return hashlib.sha256(data).hexdigest()

    def sign_request(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        body: bytes = b"",
        query_params: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        """
        Sign an HTTP request with AWS Signature V4.

        Args:
            method: HTTP method (GET, PUT, etc.)
            url: Full URL to sign
            headers: Request headers (must include 'host')
            body: Request body (default empty)
            query_params: Query parameters

        Returns:
            Headers dict with Authorization header added
        """
        if query_params is None:
            query_params = {}

        # Parse URL
        parsed = urlparse(url)
        path = parsed.path or "/"

        # Get current time
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")

        # Add required headers
        headers = dict(headers)
        headers["x-amz-date"] = amz_date
        headers["x-amz-content-sha256"] = self._sha256_hash(body)

        # Build canonical request
        canonical_uri = self._get_canonical_uri(path)
        canonical_query_string = self._get_canonical_query_string(query_params)
        canonical_headers, signed_headers = self._get_canonical_headers(headers)
        payload_hash = self._sha256_hash(body)

        canonical_request = "\n".join([
            method.upper(),
            canonical_uri,
            canonical_query_string,
            canonical_headers,
            signed_headers,
            payload_hash,
        ])

        # Build string to sign
        credential_scope = f"{date_stamp}/{self.region}/{self.SERVICE}/aws4_request"
        string_to_sign = "\n".join([
            self.ALGORITHM,
            amz_date,
            credential_scope,
            self._sha256_hash(canonical_request.encode("utf-8")),
        ])

        # Calculate signature
        signing_key = self._get_signature_key(date_stamp)
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        # Build authorization header
        authorization = (
            f"{self.ALGORITHM} "
            f"Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )

        headers["Authorization"] = authorization
        return headers

    def presign_url(
        self,
        method: str,
        url: str,
        expires_in: int = 3600,
        query_params: Optional[Dict[str, str]] = None,
        request_date: Optional[datetime] = None,
    ) -> str:
        """
        Generate a presigned URL for an S3 operation.

        Args:
            method: HTTP method (GET, PUT)
            url: Full URL to presign
            expires_in: URL expiration time in seconds (default 1 hour)
            query_params: Additional query parameters
            request_date: Custom signing date (for testing/reproducibility)

        Returns:
            Presigned URL string
        """
        if query_params is None:
            query_params = {}

        # Parse URL
        parsed = urlparse(url)
        path = parsed.path or "/"
        host = parsed.netloc

        # Use provided date or current time
        now = request_date if request_date else datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")

        # Build credential scope
        credential_scope = f"{date_stamp}/{self.region}/{self.SERVICE}/aws4_request"

        # Build query parameters for presigning
        presign_params = {
            "X-Amz-Algorithm": self.ALGORITHM,
            "X-Amz-Credential": f"{self.access_key}/{credential_scope}",
            "X-Amz-Date": amz_date,
            "X-Amz-Expires": str(expires_in),
            "X-Amz-SignedHeaders": "host",
            **query_params,
        }

        # Build canonical request
        canonical_uri = self._get_canonical_uri(path)
        canonical_query_string = self._get_canonical_query_string(presign_params)
        canonical_headers = f"host:{host}\n"
        signed_headers = "host"
        payload_hash = "UNSIGNED-PAYLOAD"

        canonical_request = "\n".join([
            method.upper(),
            canonical_uri,
            canonical_query_string,
            canonical_headers,
            signed_headers,
            payload_hash,
        ])

        # Build string to sign
        string_to_sign = "\n".join([
            self.ALGORITHM,
            amz_date,
            credential_scope,
            self._sha256_hash(canonical_request.encode("utf-8")),
        ])

        # Calculate signature
        signing_key = self._get_signature_key(date_stamp)
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        # Build final URL
        presign_params["X-Amz-Signature"] = signature
        query_string = urlencode(presign_params, safe="-_.~")
        return f"{parsed.scheme}://{host}{path}?{query_string}"
