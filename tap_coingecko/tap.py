"""Coingecko tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_coingecko.streams.hourly import CoingeckoDailyStream, CoingeckoHourlyStream

# Define all available stream types for this tap
STREAM_TYPES = [
    CoingeckoDailyStream,
    CoingeckoHourlyStream,
]


class TapCoingecko(Tap):
    """Coingecko tap class."""

    name = "tap-coingecko"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "token",
            th.ArrayType(th.StringType),
            required=True,
            description="The name of the token to import the price history of",
            default=["ethereum", "solana"],
        ),
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            description="Coingecko's api url",
            default="https://api.coingecko.com/api/v3",
        ),
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            description="Your CoinGecko API key",
        ),
        th.Property(
            "start_date",
            th.StringType,
            required=True,
            description="First date to obtain token price for",
            default="2022-03-01",
        ),
        th.Property(
            "wait_time_between_requests",
            th.IntegerType,
            required=True,
            description="Number of seconds to wait between requests",
            default=5,
        ),
        # Add optional parameters for more granular control
        th.Property(
            "days",
            th.StringType,
            description="Number of days of data to retrieve (integer or 'max')",
            default="1",
        ),
        th.Property(
            "vs_currency",
            th.StringType,
            description="Target currency for market data",
            default="usd",
        ),
        th.Property(
            "interval",
            th.StringType,
            description="Data granularity interval (leave blank for auto granularity)",
        ),
        th.Property(
            "precision",
            th.StringType,
            description="Decimal place for currency price value",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
