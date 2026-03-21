"""Client-side exception hierarchy for CAScadq."""


class CascadqError(Exception):
    """Base exception for all CAScadq client errors."""


class QueueNotFoundError(CascadqError):
    """The requested queue does not exist."""


class QueueAlreadyExistsError(CascadqError):
    """A queue with this name already exists."""


class TaskNotFoundError(CascadqError):
    """The requested task does not exist in the queue."""


class TaskNotClaimedError(CascadqError):
    """The task is not in 'claimed' status."""


class BrokerFencedError(CascadqError):
    """This broker has been fenced out by another broker's CAS write."""


class FlushExhaustedError(CascadqError):
    """Flush retries exhausted — broker cannot persist in-memory state.

    Unlike BrokerFencedError, this does not mean another broker took
    over.  The broker's in-memory state may be valid but cannot be
    written to storage.  Recovery requires a restart from durable state.
    """


class PayloadValidationError(CascadqError):
    """The task payload does not match the queue's JSON Schema."""
