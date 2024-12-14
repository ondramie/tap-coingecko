"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import os

from singer_sdk.testing import get_tap_test_class

from tap_coingecko.tap import TapCoingecko

YESTERDAY = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime(
    "%Y-%m-%d"
)
DAY_BEFORE_YESTERDAY = (
    datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=10)
).strftime("%Y-%m-%d")

SAMPLE_CONFIG = {
    "token": "ethereum",
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
TestTapCoingecko = get_tap_test_class(
    tap_class=TapCoingecko,
    config=get_test_config(),
)

# TODO: Create additional tests as appropriate for your tap.
