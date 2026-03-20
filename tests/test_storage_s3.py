"""Tests for the S3-compatible object store backend.

Uses a mock aiobotocore client — no real credentials needed.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cascadq.errors import ConflictError
from botocore.exceptions import ClientError

from cascadq.storage.s3 import S3ObjectStore, _strip_etag


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(code: str) -> ClientError:
    """Build a botocore ClientError with the given error code."""
    return ClientError(
        {"Error": {"Code": code, "Message": "test"}},
        "TestOperation",
    )


def _put_response(etag: str = '"abc123"') -> dict:
    return {"ResponseMetadata": {"HTTPHeaders": {"etag": etag}}}


def _get_response(data: bytes, etag: str = '"abc123"') -> dict:
    body = AsyncMock()
    body.read = AsyncMock(return_value=data)
    body.__aenter__ = AsyncMock(return_value=body)
    body.__aexit__ = AsyncMock(return_value=False)
    return {
        "Body": body,
        "ResponseMetadata": {"HTTPHeaders": {"etag": etag}},
    }


@pytest.fixture
def mock_client() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def store(mock_client: AsyncMock) -> S3ObjectStore:
    """Return an S3ObjectStore with the internal client pre-set (skip start)."""
    s = S3ObjectStore(
        bucket="test-bucket",
        access_key_id="fake-id",
        secret_access_key="fake-secret",
        endpoint_url="https://s3.example.com",
    )
    s._client = mock_client
    return s


# ---------------------------------------------------------------------------
# ETag stripping
# ---------------------------------------------------------------------------


class TestStripEtag:
    def test_strips_surrounding_quotes(self) -> None:
        assert _strip_etag('"abc123"') == "abc123"

    def test_no_quotes_unchanged(self) -> None:
        assert _strip_etag("abc123") == "abc123"

    def test_empty_string(self) -> None:
        assert _strip_etag('""') == ""


# ---------------------------------------------------------------------------
# read
# ---------------------------------------------------------------------------


class TestRead:
    async def test_returns_data_and_etag(
        self, store: S3ObjectStore, mock_client: AsyncMock
    ) -> None:
        mock_client.get_object.return_value = _get_response(b"hello", '"etag1"')

        data, version = await store.read("my-key")

        assert data == b"hello"
        assert version == "etag1"
        mock_client.get_object.assert_awaited_once_with(
            Bucket="test-bucket", Key="my-key"
        )

    async def test_404_raises_key_error(
        self, store: S3ObjectStore, mock_client: AsyncMock
    ) -> None:
        mock_client.get_object.side_effect = _client_error("NoSuchKey")

        with pytest.raises(KeyError):
            await store.read("missing")

    async def test_unexpected_error_propagates(
        self, store: S3ObjectStore, mock_client: AsyncMock
    ) -> None:
        mock_client.get_object.side_effect = _client_error("InternalError")

        with pytest.raises(ClientError):
            await store.read("my-key")


# ---------------------------------------------------------------------------
# write (CAS)
# ---------------------------------------------------------------------------


class TestWrite:
    async def test_returns_new_etag(
        self, store: S3ObjectStore, mock_client: AsyncMock
    ) -> None:
        mock_client.put_object.return_value = _put_response('"new-etag"')

        version = await store.write("k", b"data", "old-etag")

        assert version == "new-etag"
        mock_client.put_object.assert_awaited_once_with(
            Bucket="test-bucket",
            Key="k",
            Body=b"data",
            IfMatch='"old-etag"',
        )

    async def test_412_raises_conflict(
        self, store: S3ObjectStore, mock_client: AsyncMock
    ) -> None:
        mock_client.put_object.side_effect = _client_error("PreconditionFailed")

        with pytest.raises(ConflictError, match="version mismatch"):
            await store.write("k", b"data", "stale")

    async def test_412_code_string_raises_conflict(
        self, store: S3ObjectStore, mock_client: AsyncMock
    ) -> None:
        mock_client.put_object.side_effect = _client_error("412")

        with pytest.raises(ConflictError):
            await store.write("k", b"data", "stale")

    async def test_unexpected_error_propagates(
        self, store: S3ObjectStore, mock_client: AsyncMock
    ) -> None:
        mock_client.put_object.side_effect = _client_error("InternalError")

        with pytest.raises(ClientError):
            await store.write("k", b"data", "v1")


# ---------------------------------------------------------------------------
# write_new
# ---------------------------------------------------------------------------


class TestWriteNew:
    async def test_returns_etag(
        self, store: S3ObjectStore, mock_client: AsyncMock
    ) -> None:
        mock_client.put_object.return_value = _put_response('"first-etag"')

        version = await store.write_new("k", b"data")

        assert version == "first-etag"
        mock_client.put_object.assert_awaited_once_with(
            Bucket="test-bucket",
            Key="k",
            Body=b"data",
            IfNoneMatch="*",
        )

    async def test_412_raises_conflict(
        self, store: S3ObjectStore, mock_client: AsyncMock
    ) -> None:
        mock_client.put_object.side_effect = _client_error("PreconditionFailed")

        with pytest.raises(ConflictError, match="already exists"):
            await store.write_new("k", b"data")

    async def test_unexpected_error_propagates(
        self, store: S3ObjectStore, mock_client: AsyncMock
    ) -> None:
        mock_client.put_object.side_effect = _client_error("InternalError")

        with pytest.raises(ClientError):
            await store.write_new("k", b"data")


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


class TestDelete:
    async def test_calls_delete_object(
        self, store: S3ObjectStore, mock_client: AsyncMock
    ) -> None:
        await store.delete("k")

        mock_client.delete_object.assert_awaited_once_with(
            Bucket="test-bucket", Key="k"
        )


# ---------------------------------------------------------------------------
# list_prefix
# ---------------------------------------------------------------------------


class TestListPrefix:
    async def test_returns_keys(
        self, store: S3ObjectStore, mock_client: AsyncMock
    ) -> None:
        mock_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "prefix/a"}, {"Key": "prefix/b"}],
            "IsTruncated": False,
        }

        keys = await store.list_prefix("prefix/")

        assert keys == ["prefix/a", "prefix/b"]

    async def test_empty_prefix(
        self, store: S3ObjectStore, mock_client: AsyncMock
    ) -> None:
        mock_client.list_objects_v2.return_value = {
            "IsTruncated": False,
        }

        keys = await store.list_prefix("nothing/")

        assert keys == []

    async def test_pagination(
        self, store: S3ObjectStore, mock_client: AsyncMock
    ) -> None:
        mock_client.list_objects_v2.side_effect = [
            {
                "Contents": [{"Key": "p/1"}],
                "IsTruncated": True,
                "NextContinuationToken": "tok1",
            },
            {
                "Contents": [{"Key": "p/2"}],
                "IsTruncated": False,
            },
        ]

        keys = await store.list_prefix("p/")

        assert keys == ["p/1", "p/2"]
        assert mock_client.list_objects_v2.await_count == 2


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    async def test_context_manager(self) -> None:
        mock_client = AsyncMock()
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.create_client.return_value = mock_ctx

        with patch(
            "aiobotocore.session.get_session", return_value=mock_session
        ):
            store = S3ObjectStore(
                bucket="b",
                access_key_id="id",
                secret_access_key="secret",
                endpoint_url="https://s3.example.com",
            )
            async with store:
                assert store._client is mock_client

            assert store._client is None

    async def test_start_close(self) -> None:
        mock_client = AsyncMock()
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.create_client.return_value = mock_ctx

        with patch(
            "aiobotocore.session.get_session", return_value=mock_session
        ):
            store = S3ObjectStore(
                bucket="b",
                access_key_id="id",
                secret_access_key="secret",
                endpoint_url="https://s3.example.com",
            )
            await store.start()
            assert store._client is mock_client
            await store.close()
            assert store._client is None
