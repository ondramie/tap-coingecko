"""Module for CoinGecko Stream.

This module implements an incremental REST stream to fetch daily
historical cryptocurrency data.
"""

import copy
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, cast

import backoff
import pendulum
import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.helpers import types
from singer_sdk.streams import RESTStream

from tap_coingecko.streams.utils import API_HEADERS, ApiType


class CoingeckoDailyStream(RESTStream):
    """RESTStream for fetching daily historical CoinGecko token data.

    This class implements incremental replication for historical
    cryptocurrency data from the CoinGecko API.
    """

    name = "coingecko_token"
    primary_keys = ["date", "token"]
    replication_key = "date"
    replication_method = "INCREMENTAL"
    is_sorted = False
    state_partitioning_keys = ["token"]

    @property
    def state_partitioning_key_values(self) -> dict[str, list[Any]]:
        """Return a dictionary of partition key names and their possible
        values."""
        return {"token": self.config["token"]}

    def get_replication_key_signpost(self, context: types.Context | None) -> datetime | Any | None:
        """Return the signpost value for the replication key (yesterday's
        date).

        Overrides the default method to return yesterday's date in UTC.

        Args
        ----
        context : types.Context | None
            Optional context for the stream.

        Returns
        -------
        datetime | Any | None
            Max allowable bookmark value for this stream's replication key.
        """
        return pendulum.yesterday(tz="UTC")

    def get_concurrent_request_parameters(self) -> Optional[Mapping[str, Any]]:
        """Return request parameters for concurrent requests based on API type.

        Returns
        -------
        Optional[Mapping[str, Any]]
            Mapping of concurrency parameters for the Pro API, or None for the free API.
        """
        match self.config["api_url"]:
            case ApiType.PRO.value:
                return {
                    "concurrency": 5,
                    "max_rate_limit": 10,
                    "rate_limit_window_size": 1.0,
                }
            case _:
                return None

    def get_request_headers(self) -> Dict[str, str]:
        """Return API request headers based on the API type and key.

        Returns
        -------
        Dict[str, str]
            A dictionary containing headers to authenticate requests.
        """
        header_key = API_HEADERS.get(self.config["api_url"])
        if header_key and self.config.get("api_key"):
            return {header_key: self.config["api_key"]}
        return {}

    @property
    def url_base(self) -> str:
        """Return the base URL for API requests.

        Returns
        -------
        str
            The base URL.
        """
        match self.config["api_url"]:
            case ApiType.PRO.value:
                return ApiType.PRO.value
            case ApiType.FREE.value:
                return ApiType.FREE.value
            case _:
                raise ValueError(f"Invalid API URL: {self.config['api_url']}. ")

    @property  # type: ignore[override]
    def path(self) -> str:
        """Return the API endpoint path for the current token.

        Returns
        -------
        str
            The API endpoint path.
        """
        if not hasattr(self, "current_token"):
            raise ValueError("No token has been set for the stream.")
        return f"/coins/{self.current_token}/history"

    def request_decorator(self, func: Callable) -> Callable:
        """Retry logic for API requests.

        Args
        ----
        func : Callable
            The function to be decorated with retry logic.

        Returns
        -------
        Callable
            A callable with retry capabilities.
        """
        return backoff.on_exception(
            backoff.expo,
            (RetriableAPIError, requests.exceptions.ReadTimeout),
            max_tries=8,
            factor=3,
        )(func)

    def request_records(self, context: Optional[Mapping[str, Any]]) -> Iterable[dict]:
        """Fetch records for all tokens from the API.

        Args
        ----
        context : Optional[Mapping[str, Any]]
            Additional parameters or metadata for the request.

        Yields
        ------
        dict
            Individual token records.
        """
        self.logger.info(f"Starting request_records with tokens: {self.config['token']}")
        for token in self.config["token"]:
            self.logger.info(f"Processing token: {token}")
            self.current_token, token_context = token, {"token": token}
            records = list(self._fetch_token_data(token_context))
            self.logger.info(f"Got {len(records)} records for token {token}")
            yield from records

    def _fetch_token_data(self, context: Optional[Mapping[str, Any]]) -> Iterable[dict]:
        """Fetch historical data for a specific token.

        Args
        ----
        context : Optional[Mapping[str, Any]]
            Additional parameters or metadata for the request.

        Yields
        ------
        dict
            Parsed response records for the given token.
        """
        self.logger.info(f"Fetching token for context: {self.context}")
        next_page_token = self.get_next_page_token(None, None, context)
        self.logger.info(f"Fetching `next_page_token`: {next_page_token}")
        if not next_page_token:
            return

        decorated_request = self.request_decorator(self._request)
        while next_page_token:
            prepared_request = self.prepare_request(context, next_page_token)
            prepared_request.headers.update(self.get_request_headers())
            self.logger.info(f"Prepared request: {prepared_request}")
            response = decorated_request(prepared_request, context)

            # Store next_page_token as instance variable for parse_response to access
            self._current_page_token = next_page_token
            for record in self.parse_response(response):
                record_with_context = record if context is None else {**record, **context}
                self._increment_stream_state(record_with_context, context=context)
                yield record_with_context

            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(response, previous_token, context)

            if next_page_token == previous_token:
                raise RuntimeError("Infinite pagination loop detected.")

            if not next_page_token:
                break

            if self.config["api_url"] != "https://pro-api.coingecko.com/api/v3":
                time.sleep(self.config["wait_time_between_requests"])

    def get_starting_replication_key_value(self, context: Optional[Mapping[str, Any]]) -> datetime:
        """Return the starting replication key value from state or config."""
        current_state = self.get_context_state(context)

        # Get bookmark for current partition if it exists
        bookmark = current_state.get("replication_key_value") if current_state else None

        if bookmark:
            self.logger.info(f"Resuming sync for token {self.current_token} from {bookmark}")
            return cast(datetime, pendulum.parse(bookmark))

        # Fall back to start_date from config
        config_start_date = self.config["start_date"]
        self.logger.info(
            f"Starting sync for token {self.current_token} from config date {config_start_date}"
        )
        return cast(datetime, pendulum.parse(config_start_date))

    def get_state_partitions(self, context: Optional[Mapping[str, Any]] = None) -> Iterable[dict]:
        """Return state partitions based on tokens."""
        for token in self.config["token"]:
            yield {"token": token}

    def get_updated_state(
        self, current_stream_state: Dict[str, Any], latest_record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update state for a token based on the latest record.

        Args
        ----
        current_stream_state : Dict[str, Any]
            The current state of the stream.
        latest_record : Dict[str, Any]
            The latest record being processed.

        Returns
        -------
        Dict[str, Any]
            The updated state dictionary.
        """
        current_stream_state = current_stream_state or {"bookmarks": {}}
        token = latest_record["token"]
        record_date = latest_record["date"]

        if isinstance(record_date, datetime):
            record_date = record_date.strftime("%Y-%m-%d")

        bookmarks = current_stream_state.get("bookmarks", {})
        if "coingecko_token" not in bookmarks:
            bookmarks["coingecko_token"] = {}

        bookmarks["coingecko_token"][token] = {
            "replication_key": self.replication_key,
            "replication_key_value": record_date,
        }
        current_stream_state["bookmarks"] = bookmarks
        return current_stream_state

    def get_next_page_token(
        self,
        response: Optional[requests.Response],
        previous_token: Optional[Any],
        context: Optional[Mapping[str, Any]],
    ) -> Optional[datetime]:
        """Return the next date token for pagination, or None if we've reached
        the signpost date."""
        self.logger.debug(f"Getting next page token with previous_token={previous_token}")
        old_token = previous_token or self.get_starting_replication_key_value(context)
        self.logger.debug(f"old_token after resolution: {old_token}")

        # Ensure old_token is cast to datetime
        if not isinstance(old_token, datetime):
            old_token = cast(datetime, pendulum.parse(old_token))
        self.logger.debug(f"old_token after parsing: {old_token}")

        signpost = self.get_replication_key_signpost(context)

        # Ensure signpost is not None and is a valid datetime
        if signpost is None:
            self.logger.error("Replication key signpost is None. Returning None.")
            return None
        if not isinstance(signpost, datetime):
            signpost = cast(datetime, pendulum.parse(signpost))
        self.logger.debug(f"signpost value: {signpost}")

        # Perform the comparison
        if old_token < signpost:
            next_page_token = old_token + timedelta(days=1)
            self.logger.debug(f"Returning next_page_token: {next_page_token}")
            return next_page_token

        self.logger.debug("Returning None because old_token >= signpost")
        return None

    def get_url_params(
        self, context: Optional[Mapping[str, Any]], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Generate URL parameters for API requests.

        Args
        ----
        context : Optional[Mapping[str, Any]]
            Additional parameters or metadata for the request.
        next_page_token : Optional[Any]
            Token representing the next page of data.

        Returns
        -------
        Dict[str, Any]
            A dictionary of URL parameters.
        """
        if next_page_token is None:
            return {"localization": "false"}

        if isinstance(next_page_token, datetime):
            return {"date": next_page_token.strftime("%d-%m-%Y"), "localization": "false"}

        raise ValueError("Invalid next_page_token type; expected datetime or None.")

    def parse_response(
        self,
        response: requests.Response,
    ) -> Iterable[dict]:
        """Parse API response.

        Args
        ----
        response : requests.Response
            The HTTP response from the API.
        next_page_token : Optional[datetime]
            Token representing the current page.

        Yields
        ------
        dict
            Parsed response records.
        """
        if self._current_page_token is None:
            raise ValueError("next_page_token cannot be None during parsing.")

        data = response.json()
        data["date"] = self._current_page_token.strftime("%Y-%m-%d")
        data["token"] = self.current_token
        return [data]

    def post_process(self, row: dict, context: Optional[Mapping[str, Any]] = None) -> dict:
        """Process row data after retrieval."""
        process_row = {
            "date": row["date"],
            "token": row["token"],
            "name": row.get("name"),
            "symbol": row.get("symbol"),
        }

        market_data = row.get("market_data", {})
        if market_data:
            current_price = market_data.get("current_price", {})
            process_row.update(
                {
                    "price_usd": current_price.get("usd"),
                    "price_btc": current_price.get("btc"),
                    "price_eth": current_price.get("eth"),
                    "market_cap_usd": market_data.get("market_cap", {}).get("usd"),
                    "total_volume_usd": market_data.get("total_volume", {}).get("usd"),
                }
            )

        community_data = row.get("community_data", {})
        process_row["community_data"] = {
            "twitter_followers": community_data.get("twitter_followers"),
            "reddit_average_posts_48h": community_data.get("reddit_average_posts_48h"),
        }

        return process_row

    schema = th.PropertiesList(
        th.Property("date", th.StringType, required=True),
        th.Property("token", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("price_usd", th.NumberType),
        th.Property("price_btc", th.NumberType),
        th.Property("price_eth", th.NumberType),
        th.Property("market_cap_usd", th.NumberType),
        th.Property("total_volume_usd", th.NumberType),
        th.Property(
            "community_data",
            th.ObjectType(
                th.Property("twitter_followers", th.NumberType),
                th.Property("reddit_average_posts_48h", th.NumberType),
            ),
        ),
    ).to_dict()
