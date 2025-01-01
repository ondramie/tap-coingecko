"""Module for CoinGecko Stream.

This module implements an incremental REST stream
to fetch daily historical cryptocurrency data.
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
    state_partitioning_keys = ["token"]

    def get_concurrent_request_parameters(self) -> Optional[Mapping[str, Any]]:
        """Return request parameters for concurrent requests based on API type.

        Returns
        -------
        Mapping[str, Any]
            Mapping of concurrency parameters for the Pro API, or None for the free API.

        """
        is_pro_api = self.config["api_url"] == "https://pro-api.coingecko.com/api/v3"
        if is_pro_api:
            return {
                "concurrency": 5,
                "max_rate_limit": 10,
                "rate_limit_window_size": 1.0,
            }
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
        return self.config["api_url"]

    @property
    def path(self) -> str:
        """Return the API endpoint path for the current token.

        Raises
        ------
        ValueError
            If `current_token` is not set.

        """
        if not hasattr(self, "current_token"):
            raise ValueError("No token has been set for the stream.")
        return f"/coins/{self.current_token}/history"

    def request_decorator(self, func: Callable) -> Callable:
        """Retry logic for API requests.

        Parameters
        ----------
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

        Parameters
        ----------
        context : Optional[Mapping[str, Any]]
            Additional parameters or metadata for the request.

        Yields
        ------
        dict
            Individual token records.

        """
        for token in self.config["token"]:
            self.current_token = token
            yield from self._fetch_token_data(context)

    def _fetch_token_data(self, context: Optional[Mapping[str, Any]]) -> Iterable[dict]:
        """Fetch historical data for a specific token.

        Parameters
        ----------
        context : Optional[Mapping[str, Any]]
            Additional parameters or metadata for the request.

        Yields
        ------
        dict
            Parsed response records for the given token.

        """
        next_page_token = self.get_next_page_token(None, None, context)
        if not next_page_token:
            return

        decorated_request = self.request_decorator(self._request)
        while next_page_token:
            prepared_request = self.prepare_request(context, next_page_token)
            prepared_request.headers.update(self.get_request_headers())
            response = decorated_request(prepared_request, context)
            yield from self.parse_response(response, next_page_token)

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

        # Retrieve the bookmark from the state
        bookmark = (
            current_state.get("bookmarks", {})
            .get("coingecko_token", {})
            .get(self.current_token, {})
            .get("replication_key_value")
        )

        if bookmark is not None:
            self.logger.info(f"Resuming sync from {bookmark}")
            return cast(datetime, pendulum.parse(bookmark))

        # Fall back to start_date from config
        config_start_date = self.config["start_date"]
        self.logger.info(f"Starting sync from config date {config_start_date}")
        return pendulum.parse(config_start_date)

    def get_updated_state(
        self, current_stream_state: Dict[str, Any], latest_record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update state for a token based on the latest record.

        Parameters
        ----------
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

        bookmarks = current_stream_state["bookmarks"]
        if "coingecko_token" not in bookmarks:
            bookmarks["coingecko_token"] = {}
        bookmarks["coingecko_token"][token] = {
            "replication_key": self.replication_key,
            "replication_key_value": record_date,
        }
        return current_stream_state

    def get_next_page_token(
        self,
        response: Optional[requests.Response],
        previous_token: Optional[Any],
        context: Optional[Mapping[str, Any]],
    ) -> Optional[datetime]:
        """Return the next date token for pagination, or None if we've reached the signpost date."""
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

        Parameters
        ----------
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
        self, response: requests.Response, next_page_token: Optional[datetime]
    ) -> Iterable[dict]:
        """Parse API response.

        Parameters
        ----------
        response : requests.Response
            The HTTP response from the API.
        next_page_token : Optional[datetime]
            Token representing the current page.

        Yields
        ------
        dict
            Parsed response records.

        """
        if next_page_token is None:
            raise ValueError("next_page_token cannot be None during parsing.")

        data = response.json()
        data["date"] = next_page_token.strftime("%Y-%m-%d")
        data["token"] = self.current_token
        return [data]

    schema = th.PropertiesList(
        th.Property("date", th.StringType, required=True),
        th.Property("token", th.StringType, required=True),
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
