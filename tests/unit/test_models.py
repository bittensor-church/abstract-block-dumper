from datetime import timedelta
from unittest.mock import patch

import pytest
from django.utils import timezone
from freezegun import freeze_time

from abstract_block_dumper.models import (
    ConditionType,
    NetuidType,
    ScheduledTask,
)
from tests.factories import (
    AllNetuidConfigFactory,
    BlockDumperConfigFactory,
    BlockDumperExecutionFactory,
    FailedTaskFactory,
    ModuloConfigFactory,
    RunningTaskFactory,
    ScheduledTaskFactory,
    SuccessTaskFactory,
)


@pytest.mark.django_db
def test_block_config_should_execute_every_block():
    config = BlockDumperConfigFactory(condition_type=ConditionType.EVERY_BLOCK)

    assert config.should_execute_at_block(1) is True
    assert config.should_execute_at_block(100) is True
    assert config.should_execute_at_block(9999) is True


@pytest.mark.django_db
def test_block_config_should_execute_modulo():
    """Test should_execute_at_block for modulo condiion."""
    config = ModuloConfigFactory(condition_params={"modulo": 10, "offset": 2})

    # modulo=10, offset=2 means trigger when (block - 2) % 10 == 0
    assert config.should_execute_at_block(2) is True
    assert config.should_execute_at_block(12) is True
    assert config.should_execute_at_block(22) is True
    assert config.should_execute_at_block(5) is False
    assert config.should_execute_at_block(15) is False


@pytest.mark.parametrize(
    "netuid_type,netuid_values,expected",
    [
        (NetuidType.NONE, [], [None]),
        (NetuidType.SINGLE, [42], [42]),
        (NetuidType.MULTIPLE, [1, 2, 3], [1, 2, 3]),
    ],
)
@pytest.mark.django_db
def test_block_config_get_netuids(netuid_type, netuid_values, expected):
    config = BlockDumperConfigFactory(netuid_type=netuid_type, netuid_values=netuid_values)
    assert config.get_netuids() == expected


@pytest.mark.django_db
def test_block_config_get_netuids_all():
    config = AllNetuidConfigFactory()

    with patch("abstract_block_dumper.models.get_all_active_netuids", return_value=[1, 5, 10]):
        netuids = config.get_netuids()

    assert netuids == [1, 5, 10]


# ScheduledTask tests
@pytest.mark.parametrize(
    "status,retry_count,max_retries_reached,expected",
    [
        (ScheduledTask.Status.FAILED, 2, False, True),  # Can retry
        (ScheduledTask.Status.FAILED, 3, False, False),  # Max retries reached
        (ScheduledTask.Status.FAILED, 1, True, False),  # Flag set
        (ScheduledTask.Status.SUCCESS, 1, False, False),  # Not failed
    ],
)
@pytest.mark.django_db
def test_scheduled_task_can_retry(status, retry_count, max_retries_reached, expected):
    config = BlockDumperConfigFactory(max_retries=3)
    task = ScheduledTaskFactory(
        config=config,
        status=status,
        retry_count=retry_count,
        max_retries_reached=max_retries_reached,
    )

    assert task.can_retry() == expected


@freeze_time("2023-01-01 12:00:00")
@pytest.mark.django_db
def test_scheduled_task_mark_as_started():
    """Test mark_as_started updates task fields."""
    task = ScheduledTaskFactory(status=ScheduledTask.Status.PENDING)

    task.mark_as_started("celery-123")

    task.refresh_from_db()
    assert task.status == ScheduledTask.Status.RUNNING
    assert task.started_at is not None
    assert task.celery_task_id == "celery-123"
    assert task.last_attempted_at is not None


@freeze_time("2023-01-01 12:00:00")
@pytest.mark.django_db
def test_scheduled_task_mark_as_success():
    """Test mark_as_success updates task and config stats."""
    task = RunningTaskFactory()
    result_data = {"result": "success"}
    duration = timedelta(seconds=30)

    with patch.object(task, "_update_block_execution_stats") as mock_update:
        task.mark_as_success(result_data, duration)

    task.refresh_from_db()
    assert task.status == ScheduledTask.Status.SUCCESS
    assert task.finished_at is not None
    assert task.result_data == result_data
    assert task.execution_duration == duration
    assert task.next_retry_at is None
    assert task.max_retries_reached is False

    mock_update.assert_called_once()


@freeze_time("2023-01-01 12:00:00")
@pytest.mark.django_db
def test_scheduled_task_mark_as_failed_no_retry():
    config = BlockDumperConfigFactory(max_retries=0)
    task = RunningTaskFactory(config=config)

    with patch.object(task, "_update_block_execution_stats"):
        task.mark_as_failed("Test error", "Traceback...", schedule_retry=True)

    task.refresh_from_db()
    assert task.status == ScheduledTask.Status.FAILED
    assert task.error_message == "Test error"
    assert task.error_traceback == "Traceback..."
    assert task.finished_at is not None
    assert task.next_retry_at is None
    assert task.max_retries_reached is True


@freeze_time("2023-01-01 12:00:00")
@pytest.mark.django_db
def test_scheduled_task_mark_as_failed_with_retry():
    task = RunningTaskFactory(retry_count=0)

    with patch.object(task, "_update_block_execution_stats"):
        task.mark_as_failed("Test error", "Traceback...", schedule_retry=True)

    task.refresh_from_db()
    assert task.status == ScheduledTask.Status.FAILED
    assert task.retry_count == 1
    assert task.next_retry_at is not None
    assert task.next_retry_at > timezone.now()
    assert task.max_retries_reached is False


@pytest.mark.django_db
def test_scheduled_task_update_block_execution_stats():
    execution = BlockDumperExecutionFactory(
        block_number=100,
        total_tasks_scheduled=2,
    )

    # Create tasks with different statuses
    config = BlockDumperConfigFactory()
    SuccessTaskFactory(config=config, block_number=100)
    task_failed = FailedTaskFactory(config=config, block_number=100)

    # Update stats
    task_failed._update_block_execution_stats()

    execution.refresh_from_db()
    assert execution.tasks_completed == 1
    assert execution.tasks_failed == 1
    assert execution.tasks_pending == 0
    assert execution.all_completed is True  # No pending tasks
    assert execution.has_failures is True
