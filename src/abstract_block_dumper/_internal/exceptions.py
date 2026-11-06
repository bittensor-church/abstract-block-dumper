class AbstractBlockDumperError(Exception):
    """Base exception for all Abstract Block Dumper errors."""

    pass


class ConditionEvaluationError(AbstractBlockDumperError):
    """Condition failed to evaluate."""

    pass


class CeleryTaskLockedError(AbstractBlockDumperError):
    """Celery task execution is locked"""

    pass


class QueueConfigurationError(AbstractBlockDumperError):
    """
    A BlockDumper queue cannot be set up.

    The broker is not supported, the project's Celery config conflicts, or a setting the
    queues need is invalid.
    """

    pass


class UnsupportedBrokerError(QueueConfigurationError):
    """A BlockDumper's Celery app sends to a broker other than RabbitMQ."""

    def __init__(self, app_name: str, driver_type: str) -> None:
        super().__init__(
            f"BlockDumper needs RabbitMQ, but Celery app {app_name!r} uses the {driver_type!r} broker. "
            "Point the broker URL at RabbitMQ (amqp://), or declare these tasks with @block_task, "
            "which runs on any broker."
        )
        self.app_name = app_name
        self.driver_type = driver_type
