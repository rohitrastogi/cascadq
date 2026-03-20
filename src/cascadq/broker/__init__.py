"""Broker package — shared helpers."""


def queue_key(prefix: str, name: str) -> str:
    """Build the object storage key for a queue file."""
    return f"{prefix}queues/{name}.json"
