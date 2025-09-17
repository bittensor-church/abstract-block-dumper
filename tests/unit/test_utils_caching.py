from unittest.mock import MagicMock, patch

from django.test import override_settings

from abstract_block_dumper.utils import (
    _get_all_active_netuids_cached,
    clear_caches,
    get_bittensor_client,
)


@override_settings(BITTENSOR_NETWORK="finney")
def test_bittensor_client_caching():
    # Clear any existing cache
    clear_caches()

    with patch("abstract_block_dumper.utils.bt.subtensor") as mock_subtensor:
        mock_client = MagicMock()
        mock_subtensor.return_value = mock_client

        # First call should create client
        client1 = get_bittensor_client()
        assert client1 is mock_client
        assert mock_subtensor.call_count == 1

        # Second call should use cache
        client2 = get_bittensor_client()
        assert client2 is mock_client
        assert client1 is client2
        assert mock_subtensor.call_count == 1  # Not called again

        # Clear cache and call again
        clear_caches()
        client3 = get_bittensor_client()
        assert client3 is mock_client
        assert mock_subtensor.call_count == 2  # Called again after cache clear


def test_netuids_time_based_caching():
    clear_caches()

    with patch("abstract_block_dumper.utils.get_bittensor_client") as mock_client_func:
        mock_client = MagicMock()
        mock_client.get_all_subnets_info.return_value = [
            MagicMock(netuid=1),
            MagicMock(netuid=2),
            MagicMock(netuid=3),
        ]
        mock_client_func.return_value = mock_client

        # Test same cache key returns cached result
        cache_key = 12345
        result1 = _get_all_active_netuids_cached(cache_key)
        result2 = _get_all_active_netuids_cached(cache_key)

        assert result1 == [1, 2, 3]
        assert result1 is result2  # Same list object from cache
        assert mock_client.get_all_subnets_info.call_count == 1

        # Test different cache key calls function again
        result3 = _get_all_active_netuids_cached(67890)
        assert result3 == [1, 2, 3]
        assert mock_client.get_all_subnets_info.call_count == 2
