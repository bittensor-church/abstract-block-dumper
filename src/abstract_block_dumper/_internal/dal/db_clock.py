"""
Database clock expressions.

Deadlines written by one process and checked by another (a worker's lease, read by the
scheduler) use the database clock on both sides, so they hold even when hosts' clocks
disagree. A column on this clock is only ever compared with this clock.
"""

from datetime import timedelta

from django.db.models import DateTimeField, ExpressionWrapper
from django.db.models.functions import Now


def db_now() -> Now:
    return Now()


def db_now_offset(seconds: float) -> ExpressionWrapper:
    """The database's current time shifted by `seconds` (negative values are in the past)."""
    # Django 3.2 cannot infer the type of `datetime + duration` on its own.
    return ExpressionWrapper(Now() + timedelta(seconds=seconds), output_field=DateTimeField())
