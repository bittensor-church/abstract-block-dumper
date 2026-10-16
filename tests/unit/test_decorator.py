from unittest.mock import patch

import pytest
from django.db import OperationalError

from abstract_block_dumper._internal.exceptions import CeleryTaskLockedError
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.decorators import block_task


@pytest.mark.parametrize("backfill_queue", ["", "   ", 1])
def test_block_task_rejects_invalid_backfill_queue(backfill_queue):
    with pytest.raises(ValueError, match="backfill_queue must be a non-empty string or None"):

        @block_task(backfill_queue=backfill_queue)
        def invalid_task(block_number: int):
            return block_number


class _DriverError(Exception):
    """Stand-in for a psycopg exception carrying a SQLSTATE."""

    def __init__(self, *, sqlstate: str | None = None) -> None:
        super().__init__("driver error")
        self.sqlstate = sqlstate


def _operational_error(driver_error: Exception | None) -> OperationalError:
    """Raise and catch the way Django's DatabaseErrorWrapper re-raises driver errors."""
    try:
        raise OperationalError("select for update failed") from driver_error
    except OperationalError as exc:
        return exc


def _claimable_task(block_number: int) -> int:
    return block_number


@pytest.mark.django_db
def test_lock_contention_is_reported_as_a_locked_task():
    celery_task = block_task(_claimable_task)
    contention = _operational_error(_DriverError(sqlstate="55P03"))

    with patch.object(TaskAttempt.objects, "select_for_update", side_effect=contention):
        with pytest.raises(CeleryTaskLockedError):
            celery_task(100)


@pytest.mark.django_db
def test_database_outage_while_claiming_propagates():
    """A dead database must not be mistaken for another worker holding the row."""
    celery_task = block_task(_claimable_task)
    outage = _operational_error(_DriverError(sqlstate="08006"))

    with patch.object(TaskAttempt.objects, "select_for_update", side_effect=outage):
        with pytest.raises(OperationalError):
            celery_task(100)
