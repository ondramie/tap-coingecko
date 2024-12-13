"""Tests standard tap features using the built-in SDK tests library."""

import datetime

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

# Run standard built-in tap tests from the SDK:
TestTapCoingecko = get_tap_test_class(
    tap_class=TapCoingecko,
    config=SAMPLE_CONFIG,
)

# TODO: Create additional tests as appropriate for your tap.
