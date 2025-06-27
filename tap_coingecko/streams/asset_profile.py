"""Stream for extracting a daily snapshot of comprehensive coin profile data."""
from typing import Iterable, Optional, Dict, Any, Callable
import pendulum
import requests
import backoff

from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError
from tap_coingecko.streams.utils import API_HEADERS, ApiType

class AssetProfileStream(RESTStream):
    """
    Stream for retrieving a daily snapshot of an asset's core profile.
    This stream is designed to be run frequently, but it will only capture data
    ONCE PER DAY per token. It is the definitive source for all current social
    media stats, proprietary CoinGecko scores, and unique metadata.
    """
    name = "asset_profile"
    primary_keys = ["id", "snapshot_date"]
    replication_method = "INCREMENTAL"
    replication_key = "snapshot_date"
    state_partitioning_keys = ["token"]
    
    # The path will be formatted for each token during the request cycle.
    path = "/coins/{token}"

    @property
    def url_base(self) -> str:
        """Get the base URL for CoinGecko API requests."""
        api_url = self.config.get("api_url")
        if api_url in [ApiType.PRO.value, ApiType.FREE.value]:
            return api_url
        raise ValueError(f"Invalid `api_url` provided: {api_url}")

    def get_request_headers(self) -> Dict[str, str]:
        """Return HTTP headers for requests, including authentication."""
        headers = {}
        header_key = API_HEADERS.get(self.config["api_url"])
        api_key = self.config.get("api_key")

        if not header_key:
            raise ValueError(f"Invalid API URL in config: {self.config['api_url']}")

        # Pro API requires a key.
        if self.config["api_url"] == ApiType.PRO.value and not api_key:
            raise ValueError("API key is required for the CoinGecko Pro API.")
        
        if api_key:
            headers[header_key] = api_key
            
        return headers

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """
        Requests specific data panes but explicitly EXCLUDES market_data to
        avoid redundancy with the historical coingecko_token stream.
        """
        return {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "true",
            "developer_data": "true",
            "sparkline": "false"
        }

    def request_decorator(self, func: Callable) -> Callable:
        """Decorate requests with retry logic."""
        return backoff.on_exception(
            backoff.expo,
            (RetriableAPIError, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError),
            max_tries=5,
            factor=2,
            on_giveup=lambda details: self.logger.error(
                f"Gave up after {details['tries']} tries calling {details['target'].__name__}"
            )
        )(func)

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """
        Request one record for each token, but only if it hasn't already been
        synced for the current UTC date.
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
            decorated_request = self.request_decorator(self._request)
            
            # Prepare a single request for this token
            prepared_request = self.prepare_request(context=token_context)
            try:
                response = decorated_request(prepared_request, token_context)
                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                yield from self.parse_response(response)
            except requests.exceptions.HTTPError as e:
                self.logger.error(f"HTTP error for token '{token_id}': {e}")
                # Decide if this should be fatal or just a skip
                # For a 404 (Not Found), we can safely skip.
                if e.response.status_code == 404:
                    self.logger.warning(f"Token '{token_id}' not found on CoinGecko. Skipping.")
                else:
                    # For other errors (like 401, 403, 500), it might be a fatal issue
                    raise FatalAPIError(f"Fatal HTTP error for token '{token_id}': {e}") from e

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parses the single record from the response, with robust JSON handling."""
        try:
            yield response.json()
        except requests.exceptions.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON from response: {response.text}")
            raise FatalAPIError("Received non-JSON response from API.") from e

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """
        Transforms the raw, nested API response into a flattened, structured record
        and injects the snapshot_date. It defensively accesses nested data.
        """
        market_data = row.get("market_data", {}) or {}
        community_data = row.get("community_data", {}) or {}
        developer_data = row.get("developer_data", {}) or {}

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
            "description": row.get("description", {}).get("en"),
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
