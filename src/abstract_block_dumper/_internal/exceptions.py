class AbstractBlockDumperError(Exception):
    """Base exception for all Abstract Block Dumper errors."""

    pass


class ConditionEvaluationError(AbstractBlockDumperError):
    """Condition failed to evaluate."""

    pass


class CeleryTaskLockedError(AbstractBlockDumperError):
    """Celery task execution is locked"""

    pass
