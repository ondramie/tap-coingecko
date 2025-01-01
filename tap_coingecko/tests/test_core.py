"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import os
from typing import Any, Dict

import pendulum
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
        tap = tap_instance  # Use the injected fixture
        stream = tap.streams["coingecko_token"]

        # Test starting from empty state
        empty_context: Dict[str, Dict[str, Any]] = {"state": {}}
        stream.current_token = "ethereum"
        start_date = stream.get_starting_replication_key_value(empty_context)
        assert start_date == pendulum.parse(tap.config["start_date"])

        # Test state updates for different tokens
        record1 = {"date": YESTERDAY, "token": "ethereum"}
        record2 = {"date": DAY_BEFORE_YESTERDAY, "token": "bitcoin"}

        state: Dict[str, Dict[str, Any]] = {}
        new_state = stream.get_updated_state(state, record1)
        new_state = stream.get_updated_state(new_state, record2)

        assert (
            new_state["bookmarks"]["coingecko_token"]["ethereum"]["replication_key_value"]
            == YESTERDAY
        )
        assert (
            new_state["bookmarks"]["coingecko_token"]["bitcoin"]["replication_key_value"]
            == DAY_BEFORE_YESTERDAY
        )
