"""Exception types for async tree traversal."""


class AsyncTreeError(Exception):
    """Base exception for async tree operations."""


class GrandTimeoutExceededError(AsyncTreeError):
    """Raised when grand timeout is exceeded during traversal."""


class AsyncTreeFetcherError(AsyncTreeError):
    """Raised when a user-provided fetcher fails after retries.

    Attributes:
        node_id: ID of the node that failed.
        original_error: The underlying exception from the fetcher.

    """

    def __init__(self, node_id: str, original_error: Exception) -> None:
        """Initialize with the failed node ID and original exception.

        Args:
            node_id: ID of the node that failed.
            original_error: The underlying fetcher exception.

        """
        super().__init__(f"Fetcher failed for node {node_id}")
        self.node_id = node_id
        self.original_error = original_error


class TraversalAbortedError(AsyncTreeError):
    """Raised when traversal is aborted due to a node error.

    Attributes:
        node_id: ID of the node that caused the abort.
        original_error: The original exception that triggered the abort.

    """

    def __init__(
        self,
        message: str,
        node_id: str,
        original_error: Exception,
    ) -> None:
        """Initialize with abort details.

        Args:
            message: Error message describing the abort reason.
            node_id: ID of the node that caused the abort.
            original_error: The original exception that triggered the abort.

        """
        super().__init__(message)
        self.node_id = node_id
        self.original_error = original_error
