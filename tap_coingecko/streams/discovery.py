"""Streams for market discovery features from CoinGecko API."""

from typing import Iterable, Dict, Optional, Any, Mapping
import pendulum
import requests

from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from tap_coingecko.streams.utils import API_HEADERS, ApiType


class BaseDiscoveryStream(RESTStream):
    """Base class for discovery streams."""

    replication_method = "INCREMENTAL"
    replication_key = "snapshot_timestamp"
    is_sorted = False

    @property
    def url_base(self) -> str:
        """Get the base URL for CoinGecko API requests."""
        api_url = self.config.get("api_url")
        if api_url in [ApiType.PRO.value, ApiType.FREE.value]:
            return api_url
        raise ValueError(f"Invalid `api_url` provided: {api_url}")

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = super().http_headers.copy()
        header_key = API_HEADERS.get(self.config["api_url"])
        api_key = self.config.get("api_key")

        if self.config["api_url"] == ApiType.PRO.value and not api_key:
            self.logger.warning("API key is recommended for Pro API discovery endpoints.")

        if header_key and api_key:
            headers[header_key] = api_key
        return headers


class NewlyListedStream(BaseDiscoveryStream):
    """Stream for retrieving the latest coins recently listed on CoinGecko."""

    name = "newly_listed"
    path = "/coins/list/new"
    primary_keys = ["snapshot_timestamp", "id"]

    schema = th.PropertiesList(
        th.Property("snapshot_timestamp", th.DateTimeType, required=True),
        th.Property("id", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("activated_at", th.DateTimeType),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Denormalize the response to create one record per new coin."""
        snapshot_ts = pendulum.now("UTC").isoformat()
        for row in response.json():
            row["snapshot_timestamp"] = snapshot_ts
            yield row

    def post_process(self, row: dict, context: Optional[Mapping[str, Any]] = None) -> dict:
        """Convert the activated_at timestamp."""
        activated_at_ts = row.get("activated_at")
        if activated_at_ts:
            row["activated_at"] = pendulum.from_timestamp(activated_at_ts)
        return row


class TopMoversStream(BaseDiscoveryStream):
    """Stream for retrieving the top 30 coins with the largest price gain and loss."""

    name = "top_movers"
    path = "/coins/top_gainers_losers"
    primary_keys = ["snapshot_timestamp", "id", "type"]

    def get_url_params(
        self, context: Optional[Mapping[str, Any]], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Set required parameters for the endpoint."""
        return {"vs_currency": "usd"}

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Denormalize the response to create separate records for gainers and losers."""
        snapshot_ts = pendulum.now("UTC").isoformat()
        data = response.json()

        for gainer in data.get("top_gainers", []):
            gainer["type"] = "gainer"
            gainer["snapshot_timestamp"] = snapshot_ts
            yield gainer

        for loser in data.get("top_losers", []):
            loser["type"] = "loser"
            loser["snapshot_timestamp"] = snapshot_ts
            yield loser

    schema = th.PropertiesList(
        th.Property("snapshot_timestamp", th.DateTimeType, required=True),
        th.Property("id", th.StringType, required=True),
        th.Property(
            "type", th.StringType, required=True, description="Indicates if 'gainer' or 'loser'."
        ),
        th.Property("name", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("image", th.StringType),
        th.Property("market_cap_rank", th.IntegerType),
        th.Property("usd", th.NumberType, description="Price in USD"),
        th.Property("usd_24h_vol", th.NumberType),
        th.Property("usd_24h_change", th.NumberType, description="Price change percentage in 24h"),
    ).to_dict()
