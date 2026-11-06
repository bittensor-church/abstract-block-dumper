from celery import Celery


def sent_time_limits(app: Celery, queue_name: str) -> list[list[float | None]]:
    """The `[hard, soft]` limits of the messages waiting in `queue_name`, which are taken off it."""
    limits = []
    with app.connection_for_read() as connection, connection.SimpleQueue(queue_name) as queue:
        while True:
            try:
                message = queue.get_nowait()
            except queue.Empty:
                return limits
            limits.append(message.headers["timelimit"])
            message.ack()
