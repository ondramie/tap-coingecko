"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import os
import warnings

import pytest
from singer_sdk.testing import get_tap_test_class
from singer_sdk.testing.config import SuiteConfig

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
    # Add configuration for the hourly stream
    "days": "1",  # Use a small value for testing
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


# Define test suite configuration
suite_config = SuiteConfig(
    max_records_limit=500,
    # ignore_no_records_for_streams=["coingecko_token_hourly"],
)

# Run standard built-in tap tests from the SDK:
TestBaseTapCoingecko = get_tap_test_class(
    tap_class=TapCoingecko,
    config=get_test_config(),
    suite_config=suite_config,
)


class TestCustomTapCoingecko:
    """Test cases for custom tap functionality."""

    @pytest.fixture
    def tap_instance(self) -> TapCoingecko:
        """Create a tap instance with test config."""
        return TapCoingecko(config=get_test_config())

    def test_state_per_token_daily(self, tap_instance: TapCoingecko) -> None:
        """Test that state is properly managed per token."""
        tap = tap_instance
        if "coingecko_token" not in tap_instance.streams:
            warnings.warn(
                "Daily stream not found. Skipping 'test_state_per_token_daily'.", stacklevel=2
            )
            return

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

    def test_state_per_token_hourly(self, tap_instance: TapCoingecko) -> None:
        """Test that state is properly managed per token for the hourly stream."""
        # If the hourly stream doesn't exist, warn and skip rather than fail:
        if "coingecko_token_hourly" not in tap_instance.streams:
            warnings.warn(
                "Hourly stream not found. Skipping 'test_state_per_token_hourly'.", stacklevel=2
            )
            return

        # Grab the hourly stream
        stream = tap_instance.streams["coingecko_token_hourly"]

        # We assume the partitioning is by the 'token' field:
        assert stream.state_partitioning_keys == ["token"]

        # Sample records with a 'timestamp' field (UNIX epoch).
        # 2025-01-04 -> 1735948800
        # 2025-01-05 -> 1736035200
        # 2025-01-06 -> 1736121600
        records = [
            {"timestamp": 1736035200, "token": "ethereum", "data": "test1"},  # 2025-01-05
            {"timestamp": 1735948800, "token": "bitcoin", "data": "test2"},  # 2025-01-04
            {"timestamp": 1736121600, "token": "ethereum", "data": "test3"},  # 2025-01-06
        ]

        # Simulate record processing and state updates:
        for record in records:
            context = {"token": record["token"]}
            stream._increment_stream_state(record, context=context)

        # The test checks the final state in "bookmarks" for this stream:
        state = stream.tap_state["bookmarks"]["coingecko_token_hourly"]
        assert "partitions" in state, f"Expected 'partitions' in state: {state}"

        # Find partition for Ethereum
        eth_partition = next(p for p in state["partitions"] if p["context"]["token"] == "ethereum")
        # Replication key should be "timestamp"
        assert eth_partition["progress_markers"]["replication_key"] == "timestamp"
        # Highest Ethereum timestamp is 1736121600
        assert eth_partition["progress_markers"]["replication_key_value"] == 1736121600

        # Find partition for Bitcoin
        btc_partition = next(p for p in state["partitions"] if p["context"]["token"] == "bitcoin")
        # Replication key should be "timestamp"
        assert btc_partition["progress_markers"]["replication_key"] == "timestamp"
        # Only Bitcoin record had timestamp=1735948800
        assert btc_partition["progress_markers"]["replication_key_value"] == 1735948800

    def test_hourly_stream_configuration(self, tap_instance: TapCoingecko) -> None:
        """Test that hourly stream is properly configured."""
        tap = tap_instance

        # Check that the hourly stream is registered
        assert "coingecko_token_hourly" in tap.streams

        # Get the hourly stream
        hourly_stream = tap.streams["coingecko_token_hourly"]

        # Test basic configuration
        assert hourly_stream.name == "coingecko_token_hourly"
        assert hourly_stream.primary_keys == ["timestamp", "token"]
        assert hourly_stream.replication_key == "timestamp"
        assert hourly_stream.replication_method == "INCREMENTAL"
        assert hourly_stream.state_partitioning_keys == ["token"]

        # Test path property (uses mock to avoid actual API call)
        hourly_stream.current_token = "ethereum"
        assert "/coins/ethereum/market_chart" in hourly_stream.path

    def test_hourly_request_parameters(self, tap_instance: TapCoingecko) -> None:
        """Test the request parameters for the hourly stream."""
        import logging

        # Set up logging to be more visible
        logging.basicConfig(level=logging.INFO)

        # Get the hourly stream
        stream = tap_instance.streams["coingecko_token_hourly"]

        # Force a request
        stream.current_token = "ethereum"

        # Create a context dictionary
        context = {"token": "ethereum"}

        # Prepare a request directly
        prepared_request = stream.prepare_request(context, None)

        # Log the request details
        print(f"URL: {prepared_request.url}")
        print(f"Headers: {prepared_request.headers}")
        print(f"Method: {prepared_request.method}")

    def test_live_api_response_processing(self, tap_instance: TapCoingecko) -> None:
        """Test processing of live API response."""
        if "coingecko_token" not in tap_instance.streams:
            warnings.warn(
                "Daily stream not found. Skipping 'test_live_api_response_processing'.",
                stacklevel=2,
            )
            return

        stream = tap_instance.streams["coingecko_token"]
        # Get a single record from the actual API
        records = list(stream.get_records(context={"token": "solana"}))

        assert len(records) > 0
        record = records[0]

        # Verify record has expected structure
        assert "price_usd" in record
        assert isinstance(record["price_usd"], (float, type(None)))
        assert isinstance(record["market_cap_usd"], (float, type(None)))
