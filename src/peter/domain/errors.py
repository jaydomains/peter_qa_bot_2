class PeterError(RuntimeError):
    """Base error for the PETER system."""


class ValidationError(PeterError):
    pass
