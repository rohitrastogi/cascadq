"""Encoding helpers for durable queue snapshots.

Queue files are stored as JSON by default. Compression is optional and
uses a small binary envelope so readers can distinguish compressed and
plain snapshots automatically.
"""

from __future__ import annotations

import zlib

from cascadq.models import QueueFile, deserialize_queue_file, serialize_queue_file

_COMPRESSED_MAGIC = b"CQZ1"


def encode_queue_file(
    queue_file: QueueFile,
    *,
    compress: bool = False,
) -> bytes:
    """Encode a queue snapshot for durable storage."""
    raw = serialize_queue_file(queue_file)
    if not compress:
        return raw
    return _COMPRESSED_MAGIC + zlib.compress(raw)


def decode_queue_file(raw: bytes) -> QueueFile:
    """Decode a durable queue snapshot, compressed or plain."""
    if raw.startswith(_COMPRESSED_MAGIC):
        raw = zlib.decompress(raw[len(_COMPRESSED_MAGIC):])
    return deserialize_queue_file(raw)
