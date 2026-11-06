from unittest.mock import patch

import pytest
from celery import Celery

from abstract_block_dumper._internal.services.utils import get_current_time_limit


def _noop() -> None:
    pass


@pytest.mark.parametrize(
    ("message_limits", "task_time_limit", "app_time_limit", "expected"),
    [
        ((60, 50), 120, 300, 60),
        (None, 120, 300, 120),
        ((None, 50), None, 300, 300),
        ((None, 50), None, None, None),
    ],
    ids=["limit sent with the message", "task default", "app default", "no hard limit"],
)
def test_time_limit_follows_the_worker_precedence(message_limits, task_time_limit, app_time_limit, expected):
    app = Celery("limits")
    app.conf.task_time_limit = app_time_limit
    task = app.task(name="limits.read", time_limit=task_time_limit)(_noop)
    task.push_request(timelimit=message_limits)

    with patch("abstract_block_dumper._internal.services.utils.current_task", task):
        assert get_current_time_limit() == expected


def test_time_limit_outside_a_celery_task_is_none():
    assert get_current_time_limit() is None
