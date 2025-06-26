
"""Stream for extracting a daily snapshot of comprehensive coin profile data."""

from typing import Iterable, Optional, Dict, Any
import pendulum

from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from tap_coingecko.streams.utils import API_HEADERS

class AssetProfileStream(RESTStream):
    """
    Stream for retrieving a daily snapshot of an asset's core profile.
    This stream is designed to be run frequently, but it will only capture data
    ONCE PER DAY per token. It is the source for all current social
    media stats, proprietary CoinGecko scores, and unique metadata.
    """
    name = "asset_profile"
    primary_keys = ["id", "snapshot_date"]
    replication_method = "INCREMENTAL"
    replication_key = "snapshot_date"
    state_partitioning_keys = ["token"]
    path = "/coins/{token}"

    @property
    def url_base(self) -> str:
        return self.config["api_url"]

    def get_request_headers(self) -> Dict[str, str]:
        header_key = API_HEADERS.get(self.config["api_url"])
        if header_key and self.config.get("api_key"):
            return {header_key: self.config["api_key"]}
        return {}

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """
        Requests specific data panes. Market data is enabled to get unique
        fields like TVL and ROI.
        """
        return {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "true",
            "developer_data": "true",
            "sparkline": "false"
        }
    
    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """
        Request one record for each token, but only if it hasn't already been
        synced for the current UTC date, enforcing daily granularity.
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
            yield from super().request_records(token_context)
    
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parses the single record from the response."""
        yield response.json()

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """
        Transforms the raw, nested API response into a flattened, structured record
        and injects the snapshot_date.
        """
        market_data = row.get("market_data", {})
        community_data = row.get("community_data", {})
        developer_data = row.get("developer_data", {})

        # Flatten the nested ROI object
        roi_data = market_data.get("roi")
        roi = {
            "times": roi_data.get("times"),
            "currency": roi_data.get("currency"),
            "percentage": roi_data.get("percentage"),
        } if roi_data else None

        return {
            "snapshot_date": pendulum.now("UTC").to_date_string(),
            "id": row.get("id"),
            "symbol": row.get("symbol"),
            "name": row.get("name"),
            "categories": row.get("categories"),
            "description": row.get("description", {}).get("en"), # Extract English description
            "links": row.get("links"),
            "market_cap_rank": row.get("market_cap_rank"),
            "country_origin": row.get("country_origin"),
            "genesis_date": row.get("genesis_date"),
            "sentiment_votes_up_percentage": row.get("sentiment_votes_up_percentage"),
            "sentiment_votes_down_percentage": row.get("sentiment_votes_down_percentage"),
            "coingecko_score": row.get("coingecko_score"),
            "developer_score": row.get("developer_score"),
            "community_score": row.get("community_score"),
            "liquidity_score": row.get("liquidity_score"),
            "public_interest_score": row.get("public_interest_score"),
            "total_value_locked": market_data.get("total_value_locked"),
            "mcap_to_tvl_ratio": market_data.get("mcap_to_tvl_ratio"),
            "fdv_to_tvl_ratio": market_data.get("fdv_to_tvl_ratio"),
            "roi": roi,
            "community_data_facebook_likes": community_data.get("facebook_likes"),
            "community_data_twitter_followers": community_data.get("twitter_followers"),
            "community_data_reddit_subscribers": community_data.get("reddit_subscribers"),
            "community_data_telegram_users": community_data.get("telegram_channel_user_count"),
            "developer_data_forks": developer_data.get("forks"),
            "developer_data_stars": developer_data.get("stars"),
            "developer_data_subscribers": developer_data.get("subscribers"),
            "developer_data_commit_count_4_weeks": developer_data.get("commit_count_4_weeks"),
        }

    schema = th.PropertiesList(
        th.Property("snapshot_date", th.DateType, required=True),
        th.Property("id", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("categories", th.ArrayType(th.StringType)),
        th.Property("description", th.StringType),
        th.Property("links", th.ObjectType()),
        th.Property("market_cap_rank", th.IntegerType),
        th.Property("country_origin", th.StringType),
        th.Property("genesis_date", th.DateType),
        th.Property("sentiment_votes_up_percentage", th.NumberType),
        th.Property("sentiment_votes_down_percentage", th.NumberType),
        th.Property("coingecko_score", th.NumberType),
        th.Property("developer_score", th.NumberType),
        th.Property("community_score", th.NumberType),
        th.Property("liquidity_score", th.NumberType),
        th.Property("public_interest_score", th.NumberType),
        th.Property("total_value_locked", th.NumberType),
        th.Property("mcap_to_tvl_ratio", th.NumberType),
        th.Property("fdv_to_tvl_ratio", th.NumberType),
        th.Property("roi", th.ObjectType(
            th.Property("times", th.NumberType),
            th.Property("currency", th.StringType),
            th.Property("percentage", th.NumberType),
        )),
        th.Property("community_data_facebook_likes", th.IntegerType),
        th.Property("community_data_twitter_followers", th.IntegerType),
        th.Property("community_data_reddit_subscribers", th.IntegerType),
        th.Property("community_data_telegram_users", th.IntegerType),
        th.Property("developer_data_forks", th.IntegerType),
        th.Property("developer_data_stars", th.IntegerType),
        th.Property("developer_data_subscribers", th.IntegerType),
        th.Property("developer_data_commit_count_4_weeks", th.IntegerType),
    ).to_dict()
