"""Stream for extracting a daily snapshot of comprehensive coin profile data."""

from typing import Any, Dict, Iterable, Optional

import pendulum
import requests
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.streams import RESTStream

from tap_coingecko.streams.utils import API_HEADERS, ApiType


class AssetProfileStream(RESTStream):
    """
    Stream for retrieving a daily snapshot of an asset's core profile.
    It runs once per day per token to capture unique qualitative, social,
    developer, and score metrics not available in the historical streams.
    """

    name = "asset_profile"
    primary_keys = ["id", "snapshot_date"]
    replication_method = "INCREMENTAL"
    replication_key = "snapshot_date"
    state_partitioning_keys = ["token"]
    path = "/coins/{token}"

    @property
    def url_base(self) -> str:
        """Get the base URL for CoinGecko API requests."""
        api_url = self.config.get("api_url")
        if api_url in [ApiType.PRO.value, ApiType.FREE.value]:
            return api_url
        raise ValueError(f"Invalid `api_url` provided: {api_url}")

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed, following the required paradigm."""
        headers = super().http_headers.copy()
        header_key = API_HEADERS.get(self.config["api_url"])
        api_key = self.config.get("api_key")

        if not header_key:
            raise ValueError(f"Invalid API URL in config: {self.config['api_url']}")
        if self.config["api_url"] == ApiType.PRO.value and not api_key:
            raise ValueError("API key is required for the CoinGecko Pro API.")

        if api_key:
            headers[header_key] = api_key
        return headers

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Requests all data panes to ensure we capture every available field."""
        return {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "true",
            "developer_data": "true",
            "sparkline": "false",
        }

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """
        Overrides the default `get_records` to implement the once-per-day logic
        and iterate through the configured tokens.
        """
        today_str = pendulum.now("UTC").to_date_string()

        for token_id in self.config.get("token", []):
            token_context = {"token": token_id}
            stream_state = self.get_context_state(token_context)
            last_synced_date = stream_state.get("replication_key_value")

            if last_synced_date and last_synced_date >= today_str:
                self.logger.info(
                    f"Skipping '{token_id}' for stream '{self.name}'. "
                    f"Already synced today ({today_str})."
                )
                continue

            self.logger.info(f"Fetching daily profile snapshot for '{token_id}'.")
            try:
                yield from super().get_records(token_context)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    self.logger.warning(f"Token '{token_id}' not found on CoinGecko. Skipping.")
                else:
                    raise FatalAPIError(f"Fatal HTTP error for '{token_id}': {e}") from e

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """
        Parses the single record from the response.
        FIX: Added error handling for invalid JSON.
        """
        try:
            yield response.json()
        except requests.exceptions.JSONDecodeError as e:
            raise FatalAPIError(f"Error decoding JSON from response: {response.text}") from e

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """
        Transforms the raw API response into a flattened, non-redundant record
        with all available unique, high-value fields.
        """
        market_data = row.get("market_data", {}) or {}
        community_data = row.get("community_data", {}) or {}
        developer_data = row.get("developer_data", {}) or {}
        roi_data = market_data.get("roi")

        return {
            "snapshot_date": pendulum.now("UTC").to_date_string(),
            "id": row.get("id"),
            "asset_platform_id": row.get("asset_platform_id"),
            "categories": row.get("categories"),
            "description": row.get("description", {}).get("en"),
            "country_origin": row.get("country_origin"),
            "genesis_date": row.get("genesis_date"),
            "market_cap_rank": row.get("market_cap_rank"),
            "watchlist_portfolio_users": row.get("watchlist_portfolio_users"),
            "sentiment_votes_up_percentage": row.get("sentiment_votes_up_percentage"),
            "sentiment_votes_down_percentage": row.get("sentiment_votes_down_percentage"),
            "total_value_locked": market_data.get("total_value_locked"),
            "mcap_to_tvl_ratio": market_data.get("mcap_to_tvl_ratio"),
            "fdv_to_tvl_ratio": market_data.get("fdv_to_tvl_ratio"),
            "roi_times": roi_data.get("times") if roi_data else None,
            "roi_percentage": roi_data.get("percentage") if roi_data else None,
            "telegram_channel_user_count": community_data.get("telegram_channel_user_count"),
            "developer_forks": developer_data.get("forks"),
            "developer_stars": developer_data.get("stars"),
            "developer_subscribers": developer_data.get("subscribers"),
            "developer_total_issues": developer_data.get("total_issues"),
            "developer_closed_issues": developer_data.get("closed_issues"),
            "developer_pull_requests_merged": developer_data.get("pull_requests_merged"),
            "developer_pull_request_contributors": developer_data.get("pull_request_contributors"),
            "developer_commit_count_4_weeks": developer_data.get("commit_count_4_weeks"),
        }

    schema = th.PropertiesList(
        th.Property("snapshot_date", th.DateType, required=True),
        th.Property("id", th.StringType, required=True),
        th.Property("asset_platform_id", th.StringType),
        th.Property("categories", th.ArrayType(th.StringType)),
        th.Property("description", th.StringType),
        th.Property("country_origin", th.StringType),
        th.Property("genesis_date", th.DateType),
        th.Property("market_cap_rank", th.IntegerType),
        th.Property("watchlist_portfolio_users", th.IntegerType),
        th.Property("sentiment_votes_up_percentage", th.NumberType),
        th.Property("sentiment_votes_down_percentage", th.NumberType),
        th.Property("total_value_locked", th.NumberType),
        th.Property("mcap_to_tvl_ratio", th.NumberType),
        th.Property("fdv_to_tvl_ratio", th.NumberType),
        th.Property("roi_times", th.NumberType),
        th.Property("roi_percentage", th.NumberType),
        th.Property("telegram_channel_user_count", th.IntegerType),
        th.Property("developer_forks", th.IntegerType),
        th.Property("developer_stars", th.IntegerType),
        th.Property("developer_subscribers", th.IntegerType),
        th.Property("developer_total_issues", th.IntegerType),
        th.Property("developer_closed_issues", th.IntegerType),
        th.Property("developer_pull_requests_merged", th.IntegerType),
        th.Property("developer_pull_request_contributors", th.IntegerType),
        th.Property("developer_commit_count_4_weeks", th.IntegerType),
    ).to_dict()
