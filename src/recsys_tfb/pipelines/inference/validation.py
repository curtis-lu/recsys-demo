"""Inference output validation."""


class ValidationError(Exception):
    """Raised when inference output fails sanity checks."""

    def __init__(self, failures: list[dict]):
        self.failures = failures
        msg = (
            f"{len(failures)} sanity check(s) failed: "
            + ", ".join(f["check"] for f in failures)
        )
        super().__init__(msg)
