class NotFoundError(Exception):
    """Raised when an entity is not found or does not exist."""

    pass


class ElementNotFoundError(NotFoundError):
    """Raised when an element of a collection is not found or does not exist."""

    pass


class IllegalArgumentError(Exception):
    """Raised when an argument is illegal."""

    pass


class UnexpectedError(Exception):
    """Raised when an unexpected condition or error occurs."""

    pass


class NotSupportedError(Exception):
    """Raised when an operation is not supported."""

    pass


class ExpectationNotMetError(Exception):
    """Raised when a configuration or some other expectation/pre-requisite condition is not met."""

    pass
