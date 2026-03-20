"""S3-compatible object storage backend."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from cascadq.errors import ConflictError
from cascadq.storage.protocol import VersionToken

if TYPE_CHECKING:
    from types import TracebackType

logger = logging.getLogger(__name__)


class S3ObjectStore:
    """S3-compatible storage backend implementing the ObjectStore protocol.

    Works with AWS S3, Cloudflare R2, MinIO, and any S3-compatible service.
    Uses aiobotocore for async access. Call ``start()`` before use and
    ``close()`` when done, or use as an async context manager.
    """

    def __init__(
        self,
        bucket: str,
        access_key_id: str,
        secret_access_key: str,
        endpoint_url: str | None = None,
        region: str = "auto",
    ) -> None:
        self._bucket = bucket
        self._endpoint_url = endpoint_url
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._region = region
        self._client_ctx: Any = None
        self._client: Any = None

    async def start(self) -> None:
        """Create the aiobotocore client session."""
        import aiobotocore.session

        session = aiobotocore.session.get_session()
        kwargs: dict[str, Any] = {
            "aws_access_key_id": self._access_key_id,
            "aws_secret_access_key": self._secret_access_key,
            "region_name": self._region,
        }
        if self._endpoint_url is not None:
            kwargs["endpoint_url"] = self._endpoint_url
        self._client_ctx = session.create_client("s3", **kwargs)
        self._client = await self._client_ctx.__aenter__()

    async def close(self) -> None:
        """Close the aiobotocore client session."""
        if self._client_ctx is not None:
            await self._client_ctx.__aexit__(None, None, None)
            self._client_ctx = None
            self._client = None

    async def __aenter__(self) -> S3ObjectStore:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def read(self, key: str) -> tuple[bytes, VersionToken]:
        """Read an object, returning ``(data, etag)``."""
        try:
            response = await self._client.get_object(
                Bucket=self._bucket, Key=key
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "NoSuchKey":
                raise KeyError(key) from exc
            raise

        async with response["Body"] as stream:
            data = await stream.read()
        return data, _strip_etag(response["ResponseMetadata"]["HTTPHeaders"]["etag"])

    async def write(
        self, key: str, data: bytes, expected_version: VersionToken
    ) -> VersionToken:
        """Conditional write using ``If-Match``. Returns the new ETag."""
        etag_value = f'"{expected_version}"'
        try:
            response = await self._client.put_object(
                Bucket=self._bucket, Key=key, Body=data, IfMatch=etag_value
            )
        except ClientError as exc:
            _raise_on_precondition(exc, f"version mismatch for key={key!r}")
            raise
        return _strip_etag(response["ResponseMetadata"]["HTTPHeaders"]["etag"])

    async def write_new(self, key: str, data: bytes) -> VersionToken:
        """Write-if-not-exists using ``If-None-Match: *``. Returns the ETag."""
        try:
            response = await self._client.put_object(
                Bucket=self._bucket, Key=key, Body=data, IfNoneMatch="*"
            )
        except ClientError as exc:
            _raise_on_precondition(exc, f"key already exists: {key!r}")
            raise
        return _strip_etag(response["ResponseMetadata"]["HTTPHeaders"]["etag"])

    async def delete(self, key: str) -> None:
        """Delete an object (no error if it doesn't exist)."""
        await self._client.delete_object(
            Bucket=self._bucket, Key=key
        )

    async def list_prefix(self, prefix: str) -> list[str]:
        """List all object keys under a prefix, handling pagination."""
        keys: list[str] = []
        kwargs: dict[str, str] = {"Bucket": self._bucket, "Prefix": prefix}

        while True:
            response = await self._client.list_objects_v2(**kwargs)
            keys.extend(obj["Key"] for obj in response.get("Contents", []))

            if not response.get("IsTruncated"):
                break
            kwargs["ContinuationToken"] = response["NextContinuationToken"]

        return keys


def _strip_etag(etag: str) -> str:
    """Strip surrounding quotes from an S3 ETag."""
    return etag.removeprefix('"').removesuffix('"')


def _raise_on_precondition(exc: ClientError, message: str) -> None:
    """Raise ``ConflictError`` if *exc* is a 412 Precondition Failed.

    S3-compatible services may report this as ``"PreconditionFailed"``
    (symbolic) or ``"412"`` (raw HTTP status code) depending on the service
    and SDK version.
    """
    code = exc.response["Error"]["Code"]
    if code in ("PreconditionFailed", "412"):
        raise ConflictError(message) from exc
