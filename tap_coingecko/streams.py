"""Stream type classes for tap-coingecko."""

import copy
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, cast

import backoff
import pendulum
import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.streams import RESTStream

API_HEADERS = {
    "https://pro-api.coingecko.com/api/v3": "x-cg-pro-api-key",
    "https://api.coingecko.com/api/v3": "x-cg-demo-api-key",
}


class CoingeckoStream(RESTStream):
    """RESTStream for fetching daily historical CoinGecko token data.

    This class implements incremental replication for historical cryptocurrency
    data from the CoinGecko API.
    """

    name = "coingecko_token"
    primary_keys = ["date", "token"]
    replication_key = "date"
    replication_method = "INCREMENTAL"
    is_sorted = False

    def get_concurrent_request_parameters(self) -> Optional[Mapping[str, Any]]:
        """Return request parameters for concurrent requests based on API type."""
        is_pro_api = self.config["api_url"] == "https://pro-api.coingecko.com/api/v3"

        if is_pro_api:
            return {
                "concurrency": 5,  # 5 concurrent requests
                "max_rate_limit": 10,  # 10 requests per second
                "rate_limit_window_size": 1.0,  # Window size in seconds
            }
        return None  # Use default non-concurrent behavior

    def get_request_headers(self) -> dict:
        """Return a dictionary of headers to include in the API request."""
        header_key = API_HEADERS.get(self.config["api_url"])
        if header_key and self.config.get("api_key"):
            return {header_key: self.config["api_key"]}
        return {}

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]

    @property
    def path(self) -> str:
        """Return the API endpoint path."""
        if not hasattr(self, "current_token"):
            raise ValueError("No token has been set for the stream")
        return f"/coins/{self.current_token}/history"

    def request_decorator(self, func: Callable) -> Callable:
        """Return a decorator that handles backoff for retryable errors."""
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (RetriableAPIError, requests.exceptions.ReadTimeout),
            max_tries=8,
            factor=3,
        )(func)
        return decorator

    def request_token_data(
        self, token: str, context: Optional[Mapping[str, Any]]
    ) -> Iterable[dict]:
        """Request historical data for a specific token."""
        self.current_token = token
        self.logger.info(f"Processing token: {token}")

        next_page_token: Any = self.get_next_page_token(None, None, context)
        if not next_page_token:
            return

        decorated_request = self.request_decorator(self._request)
        is_pro_api = self.config["api_url"] == "https://pro-api.coingecko.com/api/v3"

        while True:
            prepared_request = self.prepare_request(context, next_page_token=next_page_token)
            prepared_request.headers.update(self.get_request_headers())

            self.logger.debug(f"Prepared request URL: {prepared_request.url}")
            self.logger.debug(f"Prepared request headers: {prepared_request.headers}")

            resp = decorated_request(prepared_request, context)
            self.logger.debug(f"API response: {resp.status_code}, {resp.text}")

            yield from self.parse_response(resp, next_page_token)

            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp,
                previous_token=previous_token,
                context=context,
            )

            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )

            if not next_page_token:
                break

            # Only sleep if using free API
            if not is_pro_api:
                wait_time = self.config["wait_time_between_requests"]
                time.sleep(wait_time)

    def get_starting_replication_key_value(
        self, context: Optional[Mapping[str, Any]]
    ) -> Optional[datetime]:
        """Return the starting replication key value from state or config."""
        current_state = self.get_context_state(context)

        bookmark = (
            current_state.get("bookmarks", {})
            .get("coingecko_token", {})
            .get("replication_key_value")
        )

        if bookmark:
            self.logger.info(f"Resuming sync from {bookmark}")
            return cast(datetime, pendulum.parse(bookmark))

        # Fall back to start_date from config
        config_start_date = self.config["start_date"]
        self.logger.info(f"Starting sync from config date {config_start_date}")
        return cast(datetime, pendulum.parse(config_start_date))

    def get_updated_state(
        self,
        current_stream_state: dict,
        latest_record: dict,
    ) -> dict:
        """Return updated state based on latest record.

        State structure should match:
        {
            "bookmarks": {
                "coingecko_token": {
                    "replication_key": "date",
                    "replication_key_value": "2024-12-26"
                }
            }
        }
        """
        current_stream_state = current_stream_state or {"bookmarks": {}}

        record_value = latest_record["date"]
        if isinstance(record_value, datetime):
            record_value = record_value.strftime("%Y-%m-%d")

        current_stream_state["bookmarks"] = {
            "coingecko_token": {
                "replication_key": self.replication_key,
                "replication_key_value": record_value,
            }
        }

        return current_stream_state

    def request_records(self, context: Optional[Mapping[str, Any]]) -> Iterable[dict]:
        """Request records from the CoinGecko API.

        This method handles pagination through historical token data while
        implementing rate limiting between requests.
        """
        for token in self.config["token"]:
            yield from self.request_token_data(token, context)

    def get_next_page_token(
        self,
        response: Optional[requests.Response],
        previous_token: Optional[Any],
        context: Optional[Mapping[str, Any]],
    ) -> Any:
        """Return the next date token for pagination, or None if we've reached the signpost date."""
        self.logger.debug(f"Getting next page token with previous_token={previous_token}")
        old_token = (
            previous_token
            or self.get_starting_replication_key_value(context)
            or self.config["start_date"]
        )
        self.logger.debug(f"old_token after resolution: {old_token}")

        if isinstance(old_token, str):
            old_token = cast(datetime, pendulum.parse(old_token))
        self.logger.debug(f"old_token after parsing: {old_token}")

        signpost = self.get_replication_key_signpost(context)
        self.logger.debug(f"signpost value: {signpost}")
        self.logger.debug(f"comparison result: {old_token < signpost}")

        if old_token < signpost:
            next_page_token = old_token + timedelta(days=1)
            self.logger.debug(f"returning next_page_token: {next_page_token}")
            return next_page_token

        self.logger.debug("returning None because old_token >= signpost")
        return None

    def get_url_params(
        self,
        context: Optional[Mapping[str, Any]],
        next_page_token: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        if next_page_token is None:
            return {}

        return {"date": next_page_token.strftime("%d-%m-%Y"), "localization": "false"}

    def get_replication_key_signpost(
        self,
        context: Optional[Mapping[str, Any]],
    ) -> datetime:
        """Return the signpost value for the replication key."""
        return pendulum.yesterday(tz="UTC")

    # type: ignore[override]
    def parse_response(
        self, response: requests.Response, next_page_token: Optional[datetime]
    ) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        self.logger.debug(f"Raw API response: {response.json()}")
        resp_json = response.json()
        resp_json["date"] = next_page_token
        yield resp_json  # Only one row per query

    def post_process(self, row: dict, context: Optional[Mapping[str, Any]] = None) -> dict:
        """Process row data after retrieval."""
        process_row = {
            "date": row["date"].strftime("%Y-%m-%d"),
            "token": self.current_token,
            "symbol": row["symbol"],
            "name": row["name"],
        }

        market_data = row.get("market_data")

        if market_data:
            current_price = market_data.get("current_price")
            process_row.update(
                {
                    "price_usd": current_price.get("usd"),
                    "price_btc": current_price.get("btc"),
                    "price_eth": current_price.get("eth"),
                    "market_cap_usd": market_data.get("market_cap").get("usd"),
                    "total_volume_usd": market_data.get("total_volume").get("usd"),
                }
            )

        return process_row

    schema = th.PropertiesList(
        # Identifiers
        th.Property("date", th.StringType, required=True),
        th.Property("token", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        # Price Data
        th.Property("price_usd", th.NumberType),
        th.Property("price_btc", th.NumberType),
        th.Property("price_eth", th.NumberType),
        th.Property("market_cap_usd", th.NumberType),
        th.Property("total_volume_usd", th.NumberType),
        # Community metrics
        th.Property(
            "community_data",
            th.ObjectType(
                th.Property("twitter_followers", th.NumberType),
                th.Property("reddit_average_posts_48h", th.NumberType),
                th.Property("reddit_average_comments_48h", th.NumberType),
                th.Property("reddit_subscribers", th.NumberType),
                th.Property("reddit_accounts_active_48h", th.NumberType),
            ),
        ),
        th.Property(
            "public_interest_stats",
            th.ObjectType(
                th.Property("alexa_rank", th.NumberType),
            ),
        ),
    ).to_dict()
