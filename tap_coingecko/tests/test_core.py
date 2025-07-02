"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import logging.config
import os
import warnings
from typing import Any, Dict
from unittest.mock import Mock, patch

import pendulum
import pytest
import requests
import singer_sdk.metrics
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.testing import get_tap_test_class
from singer_sdk.testing.config import SuiteConfig

from tap_coingecko.streams.asset_profile import AssetProfileStream
from tap_coingecko.streams.utils import ApiType
from tap_coingecko.tap import TapCoingecko


# Patch the problematic logging functions at the module level
def mock_load_yaml_logging_config(path: str) -> Dict[str, Any]:
    """Mock YAML logging config loader that returns a basic config."""
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"simple": {"format": "%(levelname)s:%(name)s:%(message)s"}},
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "simple",
                "stream": "ext://sys.stdout",
            }
        },
        "loggers": {"singer": {"level": "INFO", "handlers": ["console"], "propagate": False}},
        "root": {"level": "INFO", "handlers": ["console"]},
    }


def mock_setup_logging(config: Dict[str, Any], *, package: str) -> None:
    """Mock logging setup that uses basic configuration."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")


# Apply the patches immediately after import
singer_sdk.metrics._load_yaml_logging_config = mock_load_yaml_logging_config
singer_sdk.metrics._setup_logging = mock_setup_logging

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
        """Test that state is properly managed per token for the hourly
        stream."""
        # If the hourly stream doesn't exist, warn and skip rather than fail:
        if "token_price_hr" not in tap_instance.streams:
            warnings.warn(
                "Hourly stream not found. Skipping 'test_state_per_token_hourly'.", stacklevel=2
            )
            return

        # Grab the hourly stream
        stream = tap_instance.streams["token_price_hr"]

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
        state = stream.tap_state["bookmarks"]["token_price_hr"]
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
        assert "token_price_hr" in tap.streams

        # Get the hourly stream
        hourly_stream = tap.streams["token_price_hr"]

        # Test basic configuration
        assert hourly_stream.name == "token_price_hr"
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
        stream = tap_instance.streams["token_price_hr"]

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

    @pytest.mark.skipif(
        not os.getenv("TAP_COINGECKO_API_KEY"), reason="TAP_COINGECKO_API_KEY not set"
    )
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

    def test_asset_profile_stream_configuration(self, tap_instance: TapCoingecko) -> None:
        """Test that asset_profile stream is properly configured."""
        assert "asset_profile" in tap_instance.streams
        stream = tap_instance.streams["asset_profile"]
        assert stream.name == "asset_profile"
        assert stream.primary_keys == ["id", "snapshot_date"]
        assert stream.replication_method == "INCREMENTAL"
        assert stream.replication_key == "snapshot_date"

    def test_asset_profile_schema_structure(self, tap_instance: TapCoingecko) -> None:
        """Test the schema structure of the asset_profile stream."""
        stream = tap_instance.streams["asset_profile"]
        schema = stream.schema
        assert "properties" in schema
        properties = schema["properties"]

        assert "id" in properties
        assert "snapshot_date" in properties
        assert "market_cap_rank" in properties
        assert "sentiment_votes_up_percentage" in properties
        assert "developer_forks" in properties

        assert properties["id"]["type"] == ["string"]
        assert properties["snapshot_date"]["type"] == ["string"]
        assert properties["market_cap_rank"]["type"] == ["integer", "null"]
        assert properties["developer_forks"]["type"] == ["integer", "null"]

    def test_asset_profile_path_property(self, tap_instance: TapCoingecko) -> None:
        """Test the path property for asset_profile stream."""
        stream = tap_instance.streams["asset_profile"]
        context = {"token": "ethereum"}
        request = stream.prepare_request(context, next_page_token=None)
        assert "/coins/ethereum" in request.url

    def test_asset_profile_get_url_params(self, tap_instance: TapCoingecko) -> None:
        """Test URL params for asset_profile stream."""
        stream = tap_instance.streams["asset_profile"]
        params = stream.get_url_params(context=None, next_page_token=None)
        expected_params = {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "true",
            "developer_data": "true",
            "sparkline": "false",
        }
        assert params == expected_params

    def test_asset_profile_http_headers(self) -> None:
        """Test request headers for different API configurations for
        asset_profile."""
        # Test with Pro API and key
        pro_config = get_test_config()
        pro_config["api_url"] = ApiType.PRO.value
        pro_config["api_key"] = "test-pro-key"
        stream = AssetProfileStream(tap=TapCoingecko(config=pro_config))
        headers = stream.http_headers
        assert "x-cg-pro-api-key" in headers
        assert headers["x-cg-pro-api-key"] == "test-pro-key"

        # Test with Free API and no key
        free_config = get_test_config()
        free_config["api_url"] = ApiType.FREE.value
        free_config["api_key"] = ""
        stream = AssetProfileStream(tap=TapCoingecko(config=free_config))
        headers = stream.http_headers
        assert "x-cg-pro-api-key" not in headers

    def test_asset_profile_once_per_day_logic(self) -> None:
        """Test that the asset_profile stream's once-per-day logic works."""
        config = get_test_config()
        config["token"] = ["solana"]
        tap_instance = TapCoingecko(config=config)
        stream = tap_instance.streams["asset_profile"]

        today_str = pendulum.now("UTC").to_date_string()
        context = {"token": "solana"}
        state = {"replication_key_value": today_str}

        with patch.object(stream, "get_context_state", return_value=state):
            records = list(stream.get_records(context=context))
            assert len(records) == 0

    def test_asset_profile_parse_response_valid_data(self, tap_instance: TapCoingecko) -> None:
        """Test parsing a valid API response for asset_profile."""
        stream = tap_instance.streams["asset_profile"]
        mock_response = Mock()
        mock_response.json.return_value = {"id": "ethereum", "name": "Ethereum"}
        records = list(stream.parse_response(mock_response))
        assert len(records) == 1
        assert records[0]["id"] == "ethereum"

    def test_asset_profile_parse_response_invalid_json(self, tap_instance: TapCoingecko) -> None:
        """Test parsing an invalid JSON response."""
        stream = tap_instance.streams["asset_profile"]
        mock_response = Mock()
        mock_response.json.side_effect = requests.exceptions.JSONDecodeError("Invalid JSON", "", 0)
        mock_response.text = "Invalid JSON"
        with pytest.raises(FatalAPIError, match="Error decoding JSON from response"):
            list(stream.parse_response(mock_response))

    def test_asset_profile_get_records_multiple_tokens(self) -> None:
        """Test that get_records processes all configured tokens."""
        config = get_test_config()
        config["token"] = ["ethereum", "bitcoin", "solana"]
        tap_instance = TapCoingecko(config=config)
        stream = tap_instance.streams["asset_profile"]

        # FIX: Combined the two 'with' statements into one line to resolve SIM117.
        with (
            patch.object(stream, "get_context_state", return_value={}),
            patch("singer_sdk.streams.RESTStream.request_records") as mock_request_records,
        ):
            mock_request_records.side_effect = [
                [{"id": "ethereum"}],
                [{"id": "bitcoin"}],
                [{"id": "solana"}],
            ]
            records = list(stream.get_records(context=None))
            assert mock_request_records.call_count == 3
            assert len(records) == 3

    def test_asset_profile_request_records_no_tokens(self) -> None:
        """Test get_records with no tokens configured."""
        config = get_test_config()
        config["token"] = []
        tap_instance = TapCoingecko(config=config)
        stream = tap_instance.streams["asset_profile"]
        records = list(stream.get_records(context=None))
        assert len(records) == 0

    def test_asset_profile_post_process(self, tap_instance: TapCoingecko) -> None:
        """Test that post_process correctly flattens the API response."""
        stream = tap_instance.streams["asset_profile"]
        raw_record = {
            "id": "ethereum",
            "market_cap_rank": 2,
            "market_data": {"roi": {"times": 29.33, "currency": "btc", "percentage": 2933.73}},
            "community_data": {"telegram_channel_user_count": 12345},
            "developer_data": {"forks": 19618},
        }
        processed = stream.post_process(raw_record, context={})
        assert processed["market_cap_rank"] == 2
        assert processed["roi_times"] == 29.33
        assert processed["developer_forks"] == 19618
        assert "roi_currency" not in processed

    def test_asset_profile_404_handling(self) -> None:
        """Test that a 404 for a token is handled gracefully."""
        config = get_test_config()
        config["token"] = ["non-existent-token", "ethereum"]
        tap_instance = TapCoingecko(config=config)
        stream = tap_instance.streams["asset_profile"]

        http_404_error = requests.exceptions.HTTPError(response=Mock(status_code=404))

        with patch(
            "singer_sdk.streams.RESTStream.get_records",
            side_effect=[http_404_error, [{"id": "ethereum"}]],
        ) as mock_sync:
            records = list(stream.get_records(context=None))

            assert mock_sync.call_count == 2
            assert len(records) == 1
            assert records[0]["id"] == "ethereum"

    def test_trending_stream_configuration(self, tap_instance: TapCoingecko) -> None:
        """Test the configuration of the TrendingStream."""
        assert "trending" in tap_instance.streams
        stream = tap_instance.streams["trending"]
        assert stream.name == "trending"
        assert stream.path == "/search/trending"
        assert stream.replication_method == "INCREMENTAL"

    def test_trending_parse_response(self, tap_instance: TapCoingecko) -> None:
        """Test that TrendingStream correctly denormalizes its response."""
        stream = tap_instance.streams["trending"]
        mock_response = Mock()
        mock_response.json.return_value = {
            "coins": [{"item": {"id": "test1"}}, {"item": {"id": "test2"}}]
        }
        records = list(stream.parse_response(mock_response))
        assert len(records) == 2
        assert records[0]["coin_id"] == "test1"
        assert "snapshot_timestamp" in records[0]

    def test_derivatives_stream_configuration(self, tap_instance: TapCoingecko) -> None:
        """Test the configuration of the DerivativesSentimentStream."""
        assert "derivatives_sentiment" in tap_instance.streams
        stream = tap_instance.streams["derivatives_sentiment"]
        assert stream.name == "derivatives_sentiment"
        assert stream.path == "/derivatives"
        assert stream.primary_keys == ["snapshot_timestamp", "market", "symbol"]

    def test_derivatives_get_url_params(self, tap_instance: TapCoingecko) -> None:
        """Test URL params for the derivatives stream."""
        stream = tap_instance.streams["derivatives_sentiment"]
        params = stream.get_url_params(context=None, next_page_token=None)
        assert params == {"include_tickers": "unexpired"}

    def test_derivatives_post_process(self, tap_instance: TapCoingecko) -> None:
        """Test post-processing for the derivatives stream."""
        stream = tap_instance.streams["derivatives_sentiment"]
        raw_record = {
            "market": "Binance",
            "symbol": "BTC-PERP",
            "funding_rate": 0.0001,
            "price_percentage_change_24h": 1.5,
            "an_extra_field_from_api": "should be removed",
        }
        processed = stream.post_process(raw_record)
        assert processed["market"] == "Binance"
        assert processed["funding_rate"] == 0.0001
        assert "snapshot_timestamp" in processed
        assert "an_extra_field_from_api" not in processed
