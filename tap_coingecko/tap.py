"""Coingecko tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

# JSON schema typing helpers
from tap_coingecko.streams import CoingeckoStream

STREAM_TYPES = [CoingeckoStream]


class TapCoingecko(Tap):
    """Coingecko tap class."""

    name = "tap-coingecko"

    # TODO: Update this section with the actual config values you expect:
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
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = []
        for token in self.config["token"]:
            # Create a modified config for this stream
            stream_config = dict(self.config)
            stream_config["token"] = token
            stream = CoingeckoStream(tap=self, config=stream_config)
            streams.append(stream)
        return streams
