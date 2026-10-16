from unittest.mock import patch

import pytest
from django.test import override_settings

from abstract_block_dumper._internal.services.scheduler import task_scheduler_factory


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_factory_reads_network_from_bittensor_network_setting(mock_get_bittensor_client):
    """task_scheduler_factory() honors the BITTENSOR_NETWORK Django setting."""
    mock_get_bittensor_client.return_value.block = 100

    with override_settings(BITTENSOR_NETWORK="test-net"):
        scheduler = task_scheduler_factory()

    assert scheduler.bittensor_client.network == "test-net"


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_factory_explicit_network_overrides_setting(mock_get_bittensor_client):
    """An explicit network argument wins over the BITTENSOR_NETWORK setting."""
    mock_get_bittensor_client.return_value.block = 100

    with override_settings(BITTENSOR_NETWORK="from-settings"):
        scheduler = task_scheduler_factory(network="explicit")

    assert scheduler.bittensor_client.network == "explicit"


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_factory_uses_the_default_stuck_task_sweep_settings(mock_get_bittensor_client):
    mock_get_bittensor_client.return_value.block = 100

    scheduler = task_scheduler_factory()

    assert scheduler.stale_task_timeout == 1800
    assert scheduler.stale_task_sweep_interval == 60
    assert scheduler.stale_task_batch_size == 200


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_factory_reads_the_stuck_task_sweep_settings(mock_get_bittensor_client):
    mock_get_bittensor_client.return_value.block = 100

    with override_settings(
        BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS=3600,
        BLOCK_DUMPER_STALE_TASK_SWEEP_INTERVAL_SECONDS=15.5,
        BLOCK_DUMPER_STALE_TASK_SWEEP_BATCH_SIZE=25,
    ):
        scheduler = task_scheduler_factory()

    assert scheduler.stale_task_timeout == 3600
    assert scheduler.stale_task_sweep_interval == 15.5
    assert scheduler.stale_task_batch_size == 25


@pytest.mark.django_db
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_factory_accepts_none_to_disable_the_sweep(mock_get_bittensor_client):
    mock_get_bittensor_client.return_value.block = 100

    with override_settings(BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS=None):
        scheduler = task_scheduler_factory()

    assert scheduler.stale_task_timeout is None


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("setting", "value", "message"),
    [
        ("BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS", 0, "BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS must be a positive number or None"),
        ("BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS", "30", "BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS must be a positive number or None"),
        ("BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS", True, "BLOCK_DUMPER_STALE_TASK_TIMEOUT_SECONDS must be a positive number or None"),
        ("BLOCK_DUMPER_STALE_TASK_SWEEP_INTERVAL_SECONDS", -1, "BLOCK_DUMPER_STALE_TASK_SWEEP_INTERVAL_SECONDS must be a positive number"),
        ("BLOCK_DUMPER_STALE_TASK_SWEEP_INTERVAL_SECONDS", None, "BLOCK_DUMPER_STALE_TASK_SWEEP_INTERVAL_SECONDS must be a positive number"),
        ("BLOCK_DUMPER_STALE_TASK_SWEEP_BATCH_SIZE", 0, "BLOCK_DUMPER_STALE_TASK_SWEEP_BATCH_SIZE must be a positive integer"),
        ("BLOCK_DUMPER_STALE_TASK_SWEEP_BATCH_SIZE", 2.5, "BLOCK_DUMPER_STALE_TASK_SWEEP_BATCH_SIZE must be a positive integer"),
        ("BLOCK_DUMPER_STALE_TASK_SWEEP_BATCH_SIZE", True, "BLOCK_DUMPER_STALE_TASK_SWEEP_BATCH_SIZE must be a positive integer"),
    ],
)
@patch("abstract_block_dumper._internal.services.utils.get_bittensor_client")
def test_factory_rejects_invalid_stuck_task_sweep_settings(mock_get_bittensor_client, setting, value, message):
    mock_get_bittensor_client.return_value.block = 100

    with override_settings(**{setting: value}), pytest.raises(ValueError, match=message):
        task_scheduler_factory()
