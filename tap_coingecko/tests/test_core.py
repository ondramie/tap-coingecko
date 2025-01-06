"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import os

import pytest
from singer_sdk.testing import get_tap_test_class

from tap_coingecko.tap import TapCoingecko

YESTERDAY = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime(
    "%Y-%m-%d"
)
DAY_BEFORE_YESTERDAY = (
    datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=10)
).strftime("%Y-%m-%d")

SAMPLE_CONFIG = {
    "token": ["ethereum"],
    "api_url": "https://api.coingecko.com/api/v3",
    "start_date": DAY_BEFORE_YESTERDAY,
    "wait_time_between_requests": 1,
    "coingecko_start_date": YESTERDAY,
}


def get_test_config() -> dict:
    """Return test config with API key from environment."""
    config = SAMPLE_CONFIG.copy()

    # Get API key from environment variable
    api_key = os.getenv("TAP_COINGECKO_API_KEY")

    if api_key:
        config["api_key"] = api_key
    else:
        # For CI/CD, you might want to use a dummy key or skip tests
        config["api_key"] = "dummy-key-for-testing"
        print("Warning: No API key found in environment. Using dummy key for testing.")

    return config


# Run standard built-in tap tests from the SDK:
TestBaseTapCoingecko = get_tap_test_class(
    tap_class=TapCoingecko,
    config=get_test_config(),
)


class TestCustomTapCoingecko:
    """Test cases for custom tap functionality."""

    @pytest.fixture
    def tap_instance(self) -> TapCoingecko:
        """Create a tap instance with test config."""
        return TapCoingecko(config=get_test_config())

    def test_state_per_token(self, tap_instance: TapCoingecko) -> None:
        """Test that state is properly managed per token."""
        tap = tap_instance
        stream = tap.streams["coingecko_token"]

        # Test that state partitioning is configured correctly
        assert stream.state_partitioning_keys == ["token"]

        # Create some test records
        records = [
            {"date": "2025-01-05", "token": "ethereum", "data": "test1"},
            {"date": "2025-01-04", "token": "bitcoin", "data": "test2"},
            {"date": "2025-01-06", "token": "ethereum", "data": "test3"},
        ]

        # Process each record and check state updates
        for record in records:
            context = {"token": record["token"]}
            # Simulate record processing
            stream._increment_stream_state(record, context=context)

        # Check the overall state structure
        state = stream.tap_state["bookmarks"]["coingecko_token"]
        assert "partitions" in state

        # Find ethereum partition
        eth_partition = next(p for p in state["partitions"] if p["context"]["token"] == "ethereum")
        assert eth_partition["progress_markers"]["replication_key"] == "date"
        assert eth_partition["progress_markers"]["replication_key_value"] == "2025-01-06"

        # Find bitcoin partition
        btc_partition = next(p for p in state["partitions"] if p["context"]["token"] == "bitcoin")
        assert btc_partition["progress_markers"]["replication_key"] == "date"
        assert btc_partition["progress_markers"]["replication_key_value"] == "2025-01-04"

    def test_live_api_response_processing(self, tap_instance: TapCoingecko) -> None:
        """Test processing of live API response."""
        stream = tap_instance.streams["coingecko_token"]

        # Get a single record from the actual API
        records = list(stream.get_records(context={"token": "solana"}))

        assert len(records) > 0
        record = records[0]

        # Verify record has expected structure
        assert "price_usd" in record
        assert isinstance(record["price_usd"], (float, type(None)))
        assert isinstance(record["market_cap_usd"], (float, type(None)))
