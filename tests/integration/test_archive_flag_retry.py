"""
The archive hint must survive a retry.

``_use_archive_network`` never reaches the database: it is stripped from
``args_json`` and travels only in the Celery message. Both retry paths rebuild
their kwargs from the ``TaskAttempt`` row, so the hint has no way back and the
worker would fall through to ``False`` — asking a pruned node for state it does
not keep, deterministically, until attempts run out.
"""

from unittest.mock import patch

import pytest

from abstract_block_dumper._internal.services.archive_hint import reset_head_cache
from abstract_block_dumper.models import TaskAttempt
from abstract_block_dumper.v1.decorators import block_task
from tests.conftest import MockedSubtensor

RECEIVED_HINTS: list[bool] = []


def archive_aware_task(block_number: int, **kwargs) -> str:
    """A consumer task that honors the hint, as the flag intends."""
    RECEIVED_HINTS.append(kwargs.get("_use_archive_network", False))
    return f"Processed block {block_number}"


def plain_task(block_number: int) -> str:
    """A consumer task that does not accept **kwargs, so it never sees the hint."""
    return f"Processed block {block_number}"


def failing_archive_aware_task(block_number: int, **kwargs) -> str:
    RECEIVED_HINTS.append(kwargs.get("_use_archive_network", False))
    msg = f"Transient RPC error for block {block_number}"
    raise RuntimeError(msg)


@pytest.fixture(autouse=True)
def clean_hint_state():
    RECEIVED_HINTS.clear()
    reset_head_cache()
    yield
    RECEIVED_HINTS.clear()
    reset_head_cache()


def patched_head(head: int):
    return patch(
        "abstract_block_dumper._internal.services.utils.get_bittensor_client",
        return_value=MockedSubtensor(current_block=head),
    )


@pytest.mark.django_db
def test_retry_kwargs_without_the_hint_still_reach_the_archive_node():
    """A retry rebuilt from the row carries no hint; the worker must recompute it."""
    celery_task = block_task(archive_aware_task)
    executable_path = f"{archive_aware_task.__module__}.{archive_aware_task.__name__}"
    TaskAttempt.objects.create(
        block_number=100,
        executable_path=executable_path,
        status=TaskAttempt.Status.PENDING,
    )

    # 900 blocks behind the head — well past the 300-block archive threshold.
    with patched_head(1000):
        celery_task(block_number=100)

    assert RECEIVED_HINTS == [True]


@pytest.mark.django_db
def test_hint_survives_a_full_dispatch_failure_retry_cycle():
    """End-to-end: dispatch with the hint, fail, re-run the kwargs the retry actually sends."""
    celery_task = block_task(failing_archive_aware_task)
    executable_path = f"{failing_archive_aware_task.__module__}.{failing_archive_aware_task.__name__}"
    TaskAttempt.objects.create(
        block_number=100,
        executable_path=executable_path,
        status=TaskAttempt.Status.PENDING,
    )

    with patched_head(1000), patch.object(celery_task, "apply_async") as apply_async:
        # Attempt 1: dispatched live, so the scheduler computed the hint for us.
        celery_task(block_number=100, _use_archive_network=True)

        retry_kwargs = apply_async.call_args.kwargs["kwargs"]
        # The row is the only source of retry kwargs, so the hint is genuinely gone.
        assert "_use_archive_network" not in retry_kwargs

        TaskAttempt.objects.filter(block_number=100).update(status=TaskAttempt.Status.PENDING)

        # Attempt 2: exactly the message the retry sends.
        celery_task(**retry_kwargs)

    assert RECEIVED_HINTS == [True, True]


@pytest.mark.django_db
def test_recent_block_is_not_pushed_onto_the_archive_node():
    """Recomputing must not send every retry to the archive node."""
    celery_task = block_task(archive_aware_task)
    executable_path = f"{archive_aware_task.__module__}.{archive_aware_task.__name__}"
    TaskAttempt.objects.create(
        block_number=990,
        executable_path=executable_path,
        status=TaskAttempt.Status.PENDING,
    )

    with patched_head(1000):
        celery_task(block_number=990)

    assert RECEIVED_HINTS == [False]


@pytest.mark.django_db
def test_task_without_var_keyword_costs_no_head_read():
    """
    Functions that do not accept **kwargs never receive the hint.

    Resolving it for them would buy nothing and cost a head read on every execution,
    so the resolver must not be reached at all.
    """
    subtensor = MockedSubtensor(current_block=1000)

    celery_task = block_task(plain_task)
    executable_path = f"{plain_task.__module__}.{plain_task.__name__}"
    TaskAttempt.objects.create(
        block_number=100,
        executable_path=executable_path,
        status=TaskAttempt.Status.PENDING,
    )

    with patch(
        "abstract_block_dumper._internal.services.utils.get_bittensor_client",
        return_value=subtensor,
    ):
        celery_task(block_number=100)

    assert TaskAttempt.objects.get(block_number=100).status == TaskAttempt.Status.SUCCESS
    assert subtensor.block_reads == 0
