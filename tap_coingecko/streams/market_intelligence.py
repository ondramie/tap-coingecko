"""Streams for unique, non-overlapping market intelligence data."""

from typing import Iterable, Dict, Optional, Any
import pendulum
import requests

from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from tap_coingecko.streams.utils import API_HEADERS, ApiType


class BaseIntelligenceStream(RESTStream):
    """Base class for streams that capture a timestamped snapshot."""

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
        """Return the HTTP headers needed for CoinGecko requests."""
        headers = super().http_headers.copy()
        header_key = API_HEADERS.get(self.config["api_url"])
        api_key = self.config.get("api_key")

        if self.config["api_url"] == ApiType.PRO.value and not api_key:
            self.logger.warning(
                "API key is not set for the CoinGecko Pro API. "
                "This may lead to authentication errors for certain endpoints."
            )

        if header_key and api_key:
            headers[header_key] = api_key
        return headers


class TrendingStream(BaseIntelligenceStream):
    """Stream for retrieving trending coins.

    Captures short-term market focus and is crucial for market intelligence.
    """

    name = "trending"
    path = "/search/trending"
    primary_keys = ["snapshot_timestamp", "coin_id"]

    schema = th.PropertiesList(
        th.Property("snapshot_timestamp", th.DateTimeType, required=True),
        th.Property("coin_id", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("market_cap_rank", th.IntegerType),
        th.Property("score", th.IntegerType, description="Trending rank, 0 is the highest."),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Denormalize the API response to create one row per trending coin."""
        snapshot_ts = pendulum.now("UTC").isoformat()
        data = response.json()
        for i, coin_data in enumerate(data.get("coins", [])):
            item = coin_data.get("item", {})
            yield {
                "snapshot_timestamp": snapshot_ts,
                "coin_id": item.get("id"),
                "name": item.get("name"),
                "symbol": item.get("symbol"),
                "market_cap_rank": item.get("market_cap_rank"),
                "score": i,
            }


class DerivativesSentimentStream(BaseIntelligenceStream):
    """Stream for retrieving derivatives tickers.

    Captures pro-level sentiment data like funding rates.
    """

    name = "derivatives_sentiment"
    primary_keys = ["snapshot_timestamp", "market", "symbol"]
    path = "/derivatives"

    schema = th.PropertiesList(
        th.Property("snapshot_timestamp", th.DateTimeType, required=True),
        th.Property("market", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("price", th.StringType),
        th.Property("price_percentage_change_24h", th.NumberType),
        th.Property("contract_type", th.StringType),
        th.Property("funding_rate", th.NumberType),
        th.Property("open_interest", th.NumberType),
        th.Property("volume_24h", th.NumberType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return request parameters for the API call."""
        # 'unexpired' is more efficient as it excludes settled contracts.
        return {"include_tickers": "unexpired"}

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Inject the ingestion timestamp and filter relevant fields."""
        return {
            "snapshot_timestamp": pendulum.now("UTC").isoformat(),
            "market": row.get("market"),
            "symbol": row.get("symbol"),
            "price": row.get("price"),
            "price_percentage_change_24h": row.get("price_percentage_change_24h"),
            "contract_type": row.get("contract_type"),
            "funding_rate": row.get("funding_rate"),
            "open_interest": row.get("open_interest"),
            "volume_24h": row.get("volume_24h"),
        }
