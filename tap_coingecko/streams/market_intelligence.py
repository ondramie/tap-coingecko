"""Streams for unique, non-overlapping market intelligence data."""

from typing import Iterable, Dict, Optional, Any
import pendulum
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from tap_coingecko.streams.utils import API_HEADERS

class BaseIntelligenceStream(RESTStream):
    """Base class for streams that capture a timestamped snapshot."""
    replication_method = "INCREMENTAL"
    replication_key = "snapshot_timestamp"
    is_sorted = False

    @property
    def url_base(self) -> str:
        return self.config["api_url"]

    def get_request_headers(self) -> Dict[str, str]:
        header_key = API_HEADERS.get(self.config["api_url"])
        if header_key and self.config.get("api_key"):
            return {header_key: self.config["api_key"]}
        return {}

class TrendingStream(BaseIntelligenceStream):
    """
    Stream for retrieving trending coins. Captures short-term market focus
    and is crucial for marketing intelligence.
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

    def parse_response(self, response) -> Iterable[dict]:
        """Denormalizes the API response to create one row per trending coin."""
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

class DerivativesStream(BaseIntelligenceStream):
    """
    Stream for retrieving derivatives tickers. Captures pro-level sentiment
    data like funding rates, giving users a competitive trading edge.
    """
    name = "derivatives_tickers"
    primary_keys = ["snapshot_timestamp", "market", "symbol"]
    path = "/derivatives"

    schema = th.PropertiesList(
        th.Property("snapshot_timestamp", th.DateTimeType, required=True),
        th.Property("market", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("price", th.StringType),
        th.Property("contract_type", th.StringType),
        th.Property("funding_rate", th.NumberType),
        th.Property("open_interest", th.NumberType),
        th.Property("volume_24h", th.NumberType),
    ).to_dict()

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        return {"include_tickers": "all"}

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Injects the ingestion timestamp into each derivatives record."""
        row["snapshot_timestamp"] = pendulum.now("UTC").isoformat()
        return row
