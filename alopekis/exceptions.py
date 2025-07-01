class FatalWorkerError(Exception):
    """Raised when a worker encounters an error that prevents any processing of the job from continuing."""
    pass

class TooManyTimeouts(Exception):
    """Raised when a worker encounters multiple timeouts when querying the OpenSearch index."""
    pass


class TooManyFailures(Exception):
    """Raised when a worker encounters multiple failures when querying the OpenSearch index."""
    pass