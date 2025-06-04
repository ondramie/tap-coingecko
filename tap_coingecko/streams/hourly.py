"""Module for CoinGecko Hourly Stream.

This module implements an incremental REST stream
to fetch hourly historical cryptocurrency data.
"""

import os
from typing import Any, Dict, Iterable, Mapping, Optional, cast

import pendulum
import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers import types
from singer_sdk.streams import RESTStream

from tap_coingecko.streams.utils import API_HEADERS, ApiType


class CoingeckoHourlyStream(RESTStream):
    """RESTStream for fetching hourly historical CoinGecko token data.

    This class implements incremental replication for hourly cryptocurrency
    data from the CoinGecko API. Note that hourly data is only available for
    Enterprise plan subscribers.
    """

    name = "token_price_hr"  # Single stream name for all tokens
    primary_keys = ["timestamp", "token"]
    replication_key = "timestamp"
    replication_method = "INCREMENTAL"
    state_partitioning_keys = ["token"]  # Enable state partitioning by token
    current_token: Optional[str] = None

    def get_request_headers(self) -> Dict[str, str]:
        """Return API request headers based on the API type and key.

        Returns
        -------
        dict
            A dictionary containing headers to authenticate requests.

        """
        header_key = API_HEADERS.get(self.config["api_url"])
        if header_key and self.config.get("api_key"):
            return {header_key: self.config["api_key"]}
        return {}

    def get_state_partitions(self, context: Optional[Mapping[str, Any]] = None) -> Iterable[dict]:
        """Return state partitions based on tokens."""
        for token in self.config["token"]:
            yield {"token": token}

    @property
    def url_base(self) -> str:
        """Return the base URL for API requests."""
        match self.config["api_url"]:
            case ApiType.PRO.value:
                return ApiType.PRO.value
            case ApiType.FREE.value:
                return ApiType.FREE.value
            case _:
                raise ValueError(f"Invalid API URL: {self.config['api_url']}. ")

    @property  # type: ignore[override]
    def path(self) -> str:
        """Return the API endpoint path for the current token's hourly data."""
        if not hasattr(self, "current_token") or not self.current_token:
            raise ValueError("No token has been set for the stream.")
        return f"/coins/{self.current_token}/market_chart"

    def get_starting_replication_key_value(self, context: Optional[Mapping[str, Any]]) -> int:
        """Return the starting replication key value from state or config."""
        # Get state for the current token partition
        current_state = self.get_context_state(context)
        self.logger.debug(f"Current state for context {context}: {current_state}")

        # Get bookmark for current partition if it exists
        bookmark = current_state.get("replication_key_value") if current_state else None
        self.logger.debug(f"Bookmark for token {self.current_token}: {bookmark}")

        if bookmark:
            self.logger.info(
                f"Resuming sync for token {self.current_token} from bookmark {bookmark}"
            )
            match bookmark:
                case int():
                    return bookmark
                case str() if bookmark.isdigit():
                    return int(bookmark)
                case _:
                    raise ValueError(f"Invalid bookmark format: {bookmark}")

        # Fall back to start_date from config
        config_start_date = self.config["start_date"]
        self.logger.info(
            f"Starting sync for token {self.current_token} from config date {config_start_date}"
        )
        # Convert the config start date directly to millisecond epoch timestamp
        return int(cast(pendulum.DateTime, pendulum.parse(config_start_date)).timestamp() * 1000)

    def get_replication_key_signpost(
        self,
        context: types.Context | None,
    ) -> int:
        """Return the signpost value for the replication key (current time in millisecond epoch)."""
        return int(pendulum.now(tz="UTC").timestamp() * 1000)

    def get_url_params(
        self, context: Optional[Mapping[str, Any]], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Generate URL parameters for API requests."""
        params: dict = {}

        # vs_currency: default to "usd" unless user specified something else
        params["vs_currency"] = self.config.get("vs_currency", "usd")

        # interval: only set if explicitly provided by user, otherwise leave blank
        if self.config.get("interval"):
            params["interval"] = self.config.get("interval")

        # precision: default to full unless user specified something else
        if self.config.get("precision"):
            params["precision"] = self.config.get("precision", "full")

        is_full_refresh = os.environ.get("MELTANO_RUN_FULL_REFRESH", "").lower() in (
            "1",
            "true",
            "t",
            "yes",
        )

        if is_full_refresh:
            params["days"] = "max"
        else:
            params["days"] = self.config.get("days", "1")

        return params

    def request_records(self, context: Optional[Mapping[str, Any]]) -> Iterable[dict]:
        """Request records for all configured tokens."""
        tokens = self.config["token"]
        self.logger.info(f"Starting sync for tokens: {tokens}")

        for token in tokens:
            self.logger.info(f"Processing token: {token}")
            self.current_token = token
            token_context = {"token": token}

            # Prepare the request
            prepared_request = self.prepare_request(token_context, None)
            prepared_request.headers.update(self.get_request_headers())
            self.logger.info(f"Making request to: {prepared_request.url}")

            # Make the API request
            response = self._request(prepared_request, token_context)
            self.logger.info(f"Response status: {response.status_code}")

            # Parse the response
            for record in self.parse_response(response):
                # Process the record
                processed_record = self.post_process(record, token_context)

                # Important: Update the state with this record
                self._increment_stream_state(processed_record, context=token_context)

                yield processed_record

            # Apply rate limiting if needed
            if self.config["api_url"] != "https://pro-api.coingecko.com/api/v3":
                import time

                sleep_time = self.config.get("wait_time_between_requests", 5)
                self.logger.info(f"Sleeping for {sleep_time} seconds...")
                time.sleep(sleep_time)

    def parse_response(
        self,
        response: requests.Response,
    ) -> Iterable[dict]:
        """Parse API response for market chart data."""
        self.logger.info(f"Parsing response for token: {self.current_token}")

        response.raise_for_status()

        data = response.json()
        self.logger.info(f"Response data keys: {list(data.keys())}")
        prices = data.get("prices", [])
        market_caps = data.get("market_caps", [])
        total_volumes = data.get("total_volumes", [])

        self.logger.info(
            f"Found {len(prices)} price datapoints, "
            f"{len(market_caps)} market cap datapoints, "
            f"and {len(total_volumes)} volume datapoints"
        )

        market_caps_dict = {item[0]: item[1] for item in market_caps}
        volumes_dict = {item[0]: item[1] for item in total_volumes}

        records = []
        for timestamp, price in prices:
            record = {
                "timestamp": timestamp,
                "iso_timestamp": pendulum.from_timestamp(timestamp / 1000).isoformat(),
                "token": self.current_token,
                "price_usd": price,
                "market_cap_usd": market_caps_dict.get(timestamp),
                "total_volume_usd": volumes_dict.get(timestamp),
            }
            records.append(record)

        return records

    def post_process(self, row: dict, context: Optional[Mapping[str, Any]] = None) -> dict:
        """Process row data after retrieval for hourly data."""
        return row

    schema = th.PropertiesList(
        th.Property("timestamp", th.IntegerType, required=True),
        th.Property("iso_timestamp", th.StringType, required=True),
        th.Property("token", th.StringType, required=True),
        th.Property("price_usd", th.NumberType),
        th.Property("market_cap_usd", th.NumberType),
        th.Property("total_volume_usd", th.NumberType),
    ).to_dict()
