"""
Rules for the numbers a project configures, and how errors, checks and tables show them.

`True` and `False` are ints to Python, but never count as numbers here.
"""

from __future__ import annotations

from typing import TypeGuard


def is_positive_number(value: object) -> bool:
    return _is_number(value) and value > 0


def is_non_negative_number(value: object) -> bool:
    return _is_number(value) and value >= 0


def is_positive_integer(value: object) -> bool:
    return _is_integer(value) and value >= 1


def is_non_negative_integer(value: object) -> bool:
    return _is_integer(value) and value >= 0


def format_number(value: float) -> str:
    """`value` without a trailing `.0`."""
    return str(int(value)) if float(value).is_integer() else str(value)


def _is_number(value: object) -> TypeGuard[int | float]:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _is_integer(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool)
