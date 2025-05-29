"""Coingecko tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_coingecko.streams.base import CoingeckoDailyStream
from tap_coingecko.streams.hourly import CoingeckoHourlyStream
from tap_coingecko.streams.categories import CoinCategoriesStream


class TapCoingecko(Tap):
    """Coingecko tap class."""

    name = "tap-coingecko"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "token",
            th.ArrayType(th.StringType),
            required=True,
            description="The name of the token(s) to import the price history of",
            default=["ethereum", "solana"],
        ),
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            description="Coingecko's BASE API URL",
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
            description="First date to obtain token price for (YYYY-MM-DD)",
            default="2022-03-01",
        ),
        th.Property(
            "wait_time_between_requests",
            th.IntegerType,
            required=True,
            description="Number of seconds to wait between requests",
            default=5,
        ),
        # 5m/1hr/1d stream specific properties
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
            default="full",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams.

        This method generates a separate stream for each token in the config.
        """
        streams = [CoingeckoDailyStream(tap=self), CoingeckoHourlyStream(tap=self), CoinCategoriesStream(tap=self)]
        return streams