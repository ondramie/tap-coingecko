"""Streams for market discovery features from CoinGecko API."""
from typing import Iterable, Dict, Optional, Any
import pendulum
import requests

from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from tap_coingecko.streams.utils import API_HEADERS, ApiType

class BaseDiscoveryStream(RESTStream):
    """Base class for discovery streams."""
    is_sorted = False

    @property
    def url_base(self) -> str:
        api_url = self.config.get("api_url")
        if api_url in [ApiType.PRO.value, ApiType.FREE.value]:
            return api_url
        raise ValueError(f"Invalid `api_url` provided: {api_url}")

    @property
    def http_headers(self) -> dict:
        headers = super().http_headers.copy()
        header_key = API_HEADERS.get(self.config["api_url"])
        api_key = self.config.get("api_key")
        
        if self.config["api_url"] == ApiType.PRO.value and not api_key:
             self.logger.warning("API key is recommended for Pro API discovery endpoints.")
        
        if header_key and api_key:
            headers[header_key] = api_key
        return headers

class AssetMarketQualityStream(BaseDiscoveryStream):
    """
    Stream for retrieving all market tickers for an asset. This is critical for
    analyzing the quality of a coin's market, using metrics like trust scores
    and bid-ask spreads.
    """
    name = "asset_market_quality"
    primary_keys = ["coin_id", "snapshot_timestamp", "market_name", "base_currency", "target_currency"]
    replication_method = "INCREMENTAL"
    replication_key = "snapshot_timestamp"
    state_partitioning_keys = ["token"]
    path = "/coins/{token}/tickers"

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        params = {"order": "trust_score_desc", "depth": "false"}
        if next_page_token:
            params["page"] = next_page_token
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        current_page = previous_token or 1
        if len(response.json().get("tickers", [])) == 100:
            return current_page + 1
        return None

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        for token_id in self.config.get("token", []):
            token_context = {"token": token_id}
            yield from super().get_records(token_context)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from response.json().get("tickers", [])

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        return {
            "snapshot_timestamp": pendulum.now("UTC").isoformat(),
            "coin_id": context.get("token") if context else None,
            "base_currency": row.get("base"),
            "target_currency": row.get("target"),
            "market_name": row.get("market", {}).get("name"),
            "last_price_usd": row.get("converted_last", {}).get("usd"),
            "volume_usd": row.get("converted_volume", {}).get("usd"),
            "trust_score": row.get("trust_score"),
            "bid_ask_spread_percentage": row.get("bid_ask_spread_percentage"),
        }

    schema = th.PropertiesList(
        th.Property("snapshot_timestamp", th.DateTimeType, required=True),
        th.Property("coin_id", th.StringType, required=True),
        th.Property("base_currency", th.StringType),
        th.Property("target_currency", th.StringType),
        th.Property("market_name", th.StringType),
        th.Property("last_price_usd", th.NumberType),
        th.Property("volume_usd", th.NumberType),
        th.Property("trust_score", th.StringType),
        th.Property("bid_ask_spread_percentage", th.NumberType),
    ).to_dict()

class TopMoversStream(BaseDiscoveryStream):
    """
    Stream for retrieving the top 30 coins with the largest price gain and loss.
    Note: This is an exclusive endpoint for paid plan subscribers.
    """
    name = "top_movers"
    path = "/coins/top_gainers_losers"
    primary_keys = ["snapshot_timestamp", "id", "type"]
    replication_method = "INCREMENTAL"
    replication_key = "snapshot_timestamp"

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        return {"vs_currency": "usd"}

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
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
        th.Property("type", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("market_cap_rank", th.IntegerType),
        th.Property("usd", th.NumberType, description="Price in USD"),
        th.Property("usd_24h_vol", th.NumberType),
        th.Property("usd_24h_change", th.NumberType, description="Price change percentage in 24h"),
    ).to_dict()

class NewlyListedStream(BaseDiscoveryStream):
    """Stream for retrieving the latest coins recently listed on CoinGecko."""
    name = "newly_listed"
    path = "/coins/list/new"
    primary_keys = ["snapshot_timestamp"] # The list itself is the record
    replication_method = "INCREMENTAL"
    replication_key = "snapshot_timestamp"

    schema = th.PropertiesList(
        th.Property("snapshot_timestamp", th.DateTimeType, required=True),
        th.Property("new_coins", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("symbol", th.StringType),
                th.Property("name", th.StringType),
                th.Property("activated_at", th.DateTimeType),
            )
        )),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """The entire list of new coins is treated as one record for that point in time."""
        raw_coins = response.json()
        processed_coins = [
            {
                "id": coin.get("id"),
                "symbol": coin.get("symbol"),
                "name": coin.get("name"),
                "activated_at": pendulum.from_timestamp(ts) if (ts := coin.get("activated_at")) else None,
            }
            for coin in raw_coins
        ]
        yield {
            "snapshot_timestamp": pendulum.now("UTC").isoformat(),
            "new_coins": processed_coins
        }
