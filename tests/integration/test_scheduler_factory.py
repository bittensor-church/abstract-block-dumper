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
