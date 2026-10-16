import pytest
from django.db import OperationalError

import abstract_block_dumper._internal.dal.django_dal as abd_dal


class _DriverError(Exception):
    """Stand-in for a psycopg exception carrying a SQLSTATE."""

    def __init__(self, *, sqlstate: str | None = None, pgcode: str | None = None) -> None:
        super().__init__("driver error")
        self.sqlstate = sqlstate
        self.pgcode = pgcode


def _wrapped_by_django(driver_error: Exception | None) -> OperationalError:
    """Raise and catch the way Django's DatabaseErrorWrapper re-raises driver errors."""
    try:
        raise OperationalError("wrapped") from driver_error
    except OperationalError as exc:
        return exc


def test_psycopg3_lock_not_available_is_contention():
    assert abd_dal.is_lock_not_available(_wrapped_by_django(_DriverError(sqlstate="55P03"))) is True


def test_psycopg2_lock_not_available_is_contention():
    assert abd_dal.is_lock_not_available(_wrapped_by_django(_DriverError(pgcode="55P03"))) is True


@pytest.mark.parametrize("sqlstate", ["08006", "57P01", None])
def test_other_codes_are_not_contention(sqlstate):
    assert abd_dal.is_lock_not_available(_wrapped_by_django(_DriverError(sqlstate=sqlstate))) is False


def test_operational_error_without_cause_is_not_contention():
    assert abd_dal.is_lock_not_available(_wrapped_by_django(None)) is False
