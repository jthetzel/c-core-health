from typing import Any


class ServiceError(Exception):
    status_code = 500

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        self.context = context


class NotFoundError(ServiceError):
    status_code = 404


class ConflictError(ServiceError):
    """The request is valid but unsafe or ambiguous in the current state."""

    status_code = 409


class MutationsDisabledError(ServiceError):
    status_code = 403
