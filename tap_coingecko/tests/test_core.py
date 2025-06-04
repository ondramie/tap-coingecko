"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import logging.config
import os
import warnings
from typing import Any, Dict

import pytest
import singer_sdk.metrics
from singer_sdk.testing import get_tap_test_class
from singer_sdk.testing.config import SuiteConfig

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
        """Test that state is properly managed per token for the hourly stream."""
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

    def test_categories_stream_configuration(self, tap_instance: TapCoingecko) -> None:
        """Test that categories stream is properly configured."""
        tap = tap_instance

        # Check that the categories stream is registered
        assert "coin_categories" in tap.streams

        # Get the categories stream
        categories_stream = tap.streams["coin_categories"]

        # Test basic configuration
        assert categories_stream.name == "coin_categories"
        assert categories_stream.primary_keys == ["coin_id"]
        assert categories_stream.replication_method == "FULL_TABLE"
        assert (
            not hasattr(categories_stream, "replication_key")
            or categories_stream.replication_key is None
        )

        # Test schema structure
        schema = categories_stream.schema
        assert "properties" in schema
        properties = schema["properties"]

        # Verify required fields exist in schema
        assert "coin_id" in properties
        assert "name" in properties
        assert "symbol" in properties
        assert "categories" in properties

        # Verify field types - JSON Schema types are arrays in Singer schemas
        assert "string" in properties["coin_id"]["type"]
        assert "string" in properties["name"]["type"]
        assert "string" in properties["symbol"]["type"]
        assert "array" in properties["categories"]["type"]

    def test_categories_stream_path_property(self, tap_instance: TapCoingecko) -> None:
        """Test the path property for categories stream."""
        categories_stream = tap_instance.streams["coin_categories"]

        # Test that accessing path without setting current_token raises an error
        with pytest.raises(ValueError, match="No token has been set"):
            _ = categories_stream.path

        # Test path with valid token
        categories_stream.current_token = "ethereum"
        assert categories_stream.path == "/coins/ethereum"

        categories_stream.current_token = "bitcoin"
        assert categories_stream.path == "/coins/bitcoin"

    def test_categories_stream_url_base_property(self, tap_instance: TapCoingecko) -> None:
        """Test the url_base property for different API configurations."""
        from tap_coingecko.streams.categories import CoinCategoriesStream

        # Test with free API
        config = get_test_config()
        config["api_url"] = "https://api.coingecko.com/api/v3"
        stream = CoinCategoriesStream(tap=TapCoingecko(config=config))
        assert stream.url_base == "https://api.coingecko.com/api/v3"

        # Test with pro API
        config = get_test_config()
        config["api_url"] = "https://pro-api.coingecko.com/api/v3"
        stream = CoinCategoriesStream(tap=TapCoingecko(config=config))
        assert stream.url_base == "https://pro-api.coingecko.com/api/v3"

        # Test with invalid API URL
        config = get_test_config()
        config["api_url"] = "https://invalid-api.com"
        stream = CoinCategoriesStream(tap=TapCoingecko(config=config))
        with pytest.raises(ValueError, match="Invalid API URL"):
            _ = stream.url_base

    def test_categories_stream_headers(self, tap_instance: TapCoingecko) -> None:
        """Test request headers for different API configurations."""
        from tap_coingecko.streams.categories import CoinCategoriesStream

        # Test headers with empty API key (free API)
        config = get_test_config()
        config["api_url"] = "https://api.coingecko.com/api/v3"
        config["api_key"] = ""  # Empty string instead of None
        stream = CoinCategoriesStream(tap=TapCoingecko(config=config))
        headers = stream.get_request_headers()
        assert headers == {}

        # Test headers with API key (free API)
        config = get_test_config()
        config["api_url"] = "https://api.coingecko.com/api/v3"
        config["api_key"] = "test-demo-key"
        stream = CoinCategoriesStream(tap=TapCoingecko(config=config))
        headers = stream.get_request_headers()
        assert headers == {"x-cg-demo-api-key": "test-demo-key"}

        # Test headers with API key (pro API)
        config = get_test_config()
        config["api_url"] = "https://pro-api.coingecko.com/api/v3"
        config["api_key"] = "test-pro-key"
        stream = CoinCategoriesStream(tap=TapCoingecko(config=config))
        headers = stream.get_request_headers()
        assert headers == {"x-cg-pro-api-key": "test-pro-key"}

    def test_categories_parse_response_valid_data(self, tap_instance: TapCoingecko) -> None:
        """Test parsing a valid API response."""
        from unittest.mock import Mock

        categories_stream = tap_instance.streams["coin_categories"]
        categories_stream.current_token = "ethereum"

        # Mock a valid response
        mock_response = Mock()
        mock_response.json.return_value = {
            "id": "ethereum",
            "name": "Ethereum",
            "symbol": "eth",
            "categories": ["Smart Contract Platform", "Ethereum Ecosystem"],
        }
        mock_response.status_code = 200
        mock_response.url = "https://api.coingecko.com/api/v3/coins/ethereum"

        # Parse the response
        records = list(categories_stream.parse_response(mock_response))

        # Verify the parsed record
        assert len(records) == 1
        record = records[0]
        assert record["coin_id"] == "ethereum"
        assert record["name"] == "Ethereum"
        assert record["symbol"] == "eth"
        assert record["categories"] == ["Smart Contract Platform", "Ethereum Ecosystem"]

    def test_categories_parse_response_missing_id(self, tap_instance: TapCoingecko) -> None:
        """Test parsing response with missing ID field."""
        from unittest.mock import Mock

        categories_stream = tap_instance.streams["coin_categories"]
        categories_stream.current_token = "ethereum"

        # Mock response missing 'id' field
        mock_response = Mock()
        mock_response.json.return_value = {
            "name": "Ethereum",
            "symbol": "eth",
            "categories": ["Smart Contract Platform"],
        }
        mock_response.status_code = 200
        mock_response.url = "https://api.coingecko.com/api/v3/coins/ethereum"

        # Parse the response
        records = list(categories_stream.parse_response(mock_response))

        # Should use current_token as coin_id when id is missing
        assert len(records) == 1
        record = records[0]
        assert record["coin_id"] == "ethereum"
        assert record["name"] == "Ethereum"

    def test_categories_parse_response_invalid_json(self, tap_instance: TapCoingecko) -> None:
        """Test parsing response with invalid JSON."""
        from unittest.mock import Mock

        import requests
        from singer_sdk.exceptions import FatalAPIError

        categories_stream = tap_instance.streams["coin_categories"]
        categories_stream.current_token = "ethereum"

        # Mock response with invalid JSON
        mock_response = Mock()
        mock_response.json.side_effect = requests.exceptions.JSONDecodeError("Invalid JSON", "", 0)
        mock_response.status_code = 200
        mock_response.url = "https://api.coingecko.com/api/v3/coins/ethereum"
        mock_response.text = "Invalid JSON response"

        # Should raise FatalAPIError for invalid JSON
        with pytest.raises(FatalAPIError, match="Failed to decode JSON response"):
            list(categories_stream.parse_response(mock_response))

    def test_categories_parse_response_empty_categories(self, tap_instance: TapCoingecko) -> None:
        """Test parsing response with missing or empty categories field."""
        from unittest.mock import Mock

        categories_stream = tap_instance.streams["coin_categories"]
        categories_stream.current_token = "bitcoin"

        # Mock response without categories field
        mock_response = Mock()
        mock_response.json.return_value = {"id": "bitcoin", "name": "Bitcoin", "symbol": "btc"}
        mock_response.status_code = 200
        mock_response.url = "https://api.coingecko.com/api/v3/coins/bitcoin"

        # Parse the response
        records = list(categories_stream.parse_response(mock_response))

        # Should default to empty list for categories
        assert len(records) == 1
        record = records[0]
        assert record["coin_id"] == "bitcoin"
        assert record["categories"] == []

    def test_categories_request_records_multiple_tokens(self, tap_instance: TapCoingecko) -> None:
        """Test that request_records processes all configured tokens."""
        from unittest.mock import patch

        from tap_coingecko.streams.categories import CoinCategoriesStream

        # Configure multiple tokens
        config = get_test_config()
        config["token"] = ["ethereum", "bitcoin", "solana"]

        # Create a fresh stream with our custom config
        categories_stream = CoinCategoriesStream(tap=TapCoingecko(config=config))

        # Mock the _process_token method to return dummy records
        with patch.object(categories_stream, "_process_token") as mock_process:
            mock_process.side_effect = [
                [
                    {
                        "coin_id": "ethereum",
                        "name": "Ethereum",
                        "symbol": "eth",
                        "categories": ["DeFi"],
                    }
                ],
                [
                    {
                        "coin_id": "bitcoin",
                        "name": "Bitcoin",
                        "symbol": "btc",
                        "categories": ["Currency"],
                    }
                ],
                [
                    {
                        "coin_id": "solana",
                        "name": "Solana",
                        "symbol": "sol",
                        "categories": ["Smart Contracts"],
                    }
                ],
            ]

            # Collect all records
            records = list(categories_stream.request_records(context=None))

            # Verify all tokens were processed
            assert len(records) == 3
            assert mock_process.call_count == 3

            # Verify token names in results
            coin_ids = [record["coin_id"] for record in records]
            assert "ethereum" in coin_ids
            assert "bitcoin" in coin_ids
            assert "solana" in coin_ids

    def test_categories_request_records_no_tokens(self, tap_instance: TapCoingecko) -> None:
        """Test request_records with no tokens configured."""
        from tap_coingecko.streams.categories import CoinCategoriesStream

        # Configure empty token list
        config = get_test_config()
        config["token"] = []

        # Create stream with empty token list
        categories_stream = CoinCategoriesStream(tap=TapCoingecko(config=config))

        # Should return empty iterator
        records = list(categories_stream.request_records(context=None))
        assert len(records) == 0

    def test_categories_rate_limiting_behavior(self, tap_instance: TapCoingecko) -> None:
        """Test rate limiting behavior for non-pro API."""
        from unittest.mock import patch

        from tap_coingecko.streams.categories import CoinCategoriesStream

        # Configure non-pro API with wait time
        config = get_test_config()
        config["api_url"] = "https://api.coingecko.com/api/v3"
        config["wait_time_between_requests"] = 2

        categories_stream = CoinCategoriesStream(tap=TapCoingecko(config=config))
        categories_stream.current_token = "ethereum"

        with patch("time.sleep") as mock_sleep:
            categories_stream._handle_rate_limiting()
            mock_sleep.assert_called_once_with(2)

        # Test pro API (should not wait)
        config = get_test_config()
        config["api_url"] = "https://pro-api.coingecko.com/api/v3"
        config["wait_time_between_requests"] = 2

        categories_stream = CoinCategoriesStream(tap=TapCoingecko(config=config))
        categories_stream.current_token = "ethereum"

        with patch("time.sleep") as mock_sleep:
            categories_stream._handle_rate_limiting()
            mock_sleep.assert_not_called()

    def test_categories_stream_get_url_params(self, tap_instance: TapCoingecko) -> None:
        """Test that get_url_params returns empty dict (no pagination)."""
        categories_stream = tap_instance.streams["coin_categories"]

        params = categories_stream.get_url_params(context=None, next_page_token=None)
        assert params == {}

        # Should still be empty even with context/token
        params = categories_stream.get_url_params(
            context={"token": "ethereum"}, next_page_token="some_token"
        )
        assert params == {}
