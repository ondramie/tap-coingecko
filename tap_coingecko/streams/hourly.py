"""Module for CoinGecko Hourly Stream.

This module implements an incremental REST stream
to fetch hourly historical cryptocurrency data.
"""

import copy
import time
from datetime import datetime
from typing import Any, Dict, Iterable, Mapping, Optional, Union, cast

import pendulum
import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from .base import CoingeckoDailyStream

TokenType = Union[int, datetime, str, None]


class CoingeckoHourlyStream(CoingeckoDailyStream):
    """RESTStream for fetching hourly historical CoinGecko token data.

    This class implements incremental replication for hourly cryptocurrency
    data from the CoinGecko API. Note that hourly data is only available for
    Enterprise plan subscribers.
    """

    name = "coingecko_token_hourly"
    primary_keys = ["timestamp", "token"]
    replication_key = "timestamp"
    replication_method = "INCREMENTAL"
    is_sorted = False
    state_partitioning_keys = ["token"]
    _current_page_token: Optional[datetime] = None

    @property  # type: ignore[override]
    def path(self) -> str:
        """Return the API endpoint path for the current token's hourly data.

        Raises
        ------
        ValueError
            If `current_token` is not set.

        """
        if not hasattr(self, "current_token"):
            raise ValueError("No token has been set for the stream.")
        return f"/coins/{self.current_token}/market_chart"

    def get_replication_key_signpost(
        self,
        context: Optional[Mapping[str, Any]],
    ) -> int:
        """Return the signpost value for the replication key (current time in millisecond epoch)."""
        now = pendulum.now(tz="UTC")
        # Convert to millisecond epoch timestamp (integer)
        epoch_ms = int(now.timestamp() * 1000)
        return epoch_ms

    def get_next_page_token(
        self,
        response: Optional[requests.Response],
        previous_token: Optional[datetime],
        context: Optional[Mapping[str, Any]],
    ) -> Optional[datetime]:
        """Return the next timestamp token for pagination."""
        self.logger.info(f"Getting next page token with previous_token={previous_token}")

        # Get the starting point (either from previous token or initial state)
        old_token = previous_token or self.get_starting_replication_key_value(context)
        self.logger.info(f"Starting with token: {old_token}")

        # For the first request, always return something to ensure API call
        if response is None:
            self.logger.info(f"Initial call - returning token: {old_token}")
            return cast(Optional[datetime], old_token)

        # Get signpost (current time) - we know this is an integer from our implementation
        signpost = self.get_replication_key_signpost(context)
        self.logger.info(f"Signpost value: {signpost}")

        # Convert old_token to int for comparison with signpost
        internal_token = self._datetime_to_ms(cast(Optional[datetime], old_token))
        if internal_token is None:
            return None

        # Check if backfill is needed
        days_param = self.config.get("days", "1")
        is_backfill = days_param == "max" or (days_param.isdigit() and int(days_param) > 30)

        # For regular cases or when we've reached the target date, we're done
        if not is_backfill or internal_token >= signpost:
            self.logger.info("No more pages needed")
            return None

        # Add 30 days in milliseconds
        chunk_size_ms = 30 * 24 * 60 * 60 * 1000  # 30 days in ms
        next_token_ms = internal_token + chunk_size_ms

        # Don't go beyond signpost
        if next_token_ms > signpost:
            next_token_ms = signpost

        # Avoid infinite loop
        if next_token_ms == internal_token:
            self.logger.info("Next token would be the same as current - no more pages")
            return None

        # Convert back to datetime
        next_token = datetime.fromtimestamp(next_token_ms / 1000)
        self.logger.info(f"Backfill in progress, next token: {next_token}")
        return next_token

    def _datetime_to_ms(self, dt: Optional[datetime]) -> Optional[int]:
        """Convert datetime to millisecond timestamp."""
        if dt is None:
            return None

        if isinstance(dt, datetime):
            return int(dt.timestamp() * 1000)

        # If we somehow got something else, try to convert it
        try:
            parsed_dt = pendulum.parse(str(dt))
            return int(parsed_dt.timestamp() * 1000)
        except Exception as e:
            self.logger.error(f"Cannot convert value to int: {e}")
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
        # Start with empty params
        params: dict = {}

        # vs_currency: default to "usd" unless user specified something else
        params["vs_currency"] = self.config.get("vs_currency", "usd")

        # interval: only set if explicitly provided by user, otherwise leave blank
        if self.config.get("interval"):
            params["interval"] = self.config.get("interval")

        # precision: default to full unless user specified something else
        if self.config.get("precision"):
            params["precision"] = self.config.get("precision")

        # Initial request (or for backfill)
        if next_page_token is None:
            # days: user-provided value, default to "1" if not specified
            # "max" indicates a backfill
            params["days"] = self.config.get("days", "1")
            self.logger.info(f"Running sync with days={params['days']}")
            return params

        # For pagination requests
        if isinstance(next_page_token, datetime):
            # Calculate the timeframe based on the starting replication key value
            start_time = self.get_starting_replication_key_value(context)
            if not isinstance(start_time, datetime):
                start_time = cast(datetime, pendulum.parse(start_time))

            # Calculate days between start_time and next_page_token
            days_diff = (next_page_token - start_time).total_seconds() / 86400

            # Add 1 to ensure we include the current day
            params["days"] = str(int(days_diff) + 1)
            return params

        raise ValueError("Invalid next_page_token type; expected datetime or None.")

    def parse_response(
        self,
        response: requests.Response,
        next_page_token: Optional[datetime],  # type: ignore[override]
    ) -> Iterable[dict]:
        """Parse API response for market chart data."""
        self.logger.info(f"Parsing response for token: {self.current_token}")

        try:
            # First check if the response was successful
            response.raise_for_status()

            # Parse the JSON data
            data = response.json()
            self.logger.info(f"Response data keys: {list(data.keys())}")

            # Check for error messages in the response
            if "error" in data:
                self.logger.error(f"API returned error: {data['error']}")
                return []

            # The market_chart endpoint returns arrays of [timestamp, value] pairs
            prices = data.get("prices", [])
            market_caps = data.get("market_caps", [])
            total_volumes = data.get("total_volumes", [])

            self.logger.info(
                f"Found {len(prices)} price datapoints, "
                f"{len(market_caps)} market cap datapoints, "
                f"and {len(total_volumes)} volume datapoints"
            )

            # If no data points found, log a warning
            if not prices:
                self.logger.warning("No price data found in response")
                if data:
                    self.logger.info(f"Response contained: {list(data.keys())}")
                return []

            # Build lookups for market_caps and volumes by timestamp
            market_caps_dict = {item[0]: item[1] for item in market_caps}
            volumes_dict = {item[0]: item[1] for item in total_volumes}

            # Process each price datapoint
            records = []
            for timestamp, price in prices:
                # Create a record with all the data for this timestamp
                record = {
                    "timestamp": timestamp,  # Keep as millisecond epoch timestamp
                    "token": self.current_token,
                    "price_usd": price,
                    "market_cap_usd": market_caps_dict.get(timestamp),
                    "total_volume_usd": volumes_dict.get(timestamp),
                }
                records.append(record)

            return records

        except Exception as e:
            self.logger.error(f"Error parsing response: {e}")
            self.logger.error(f"Response content: {response.text[:500]}...")
            # Re-raise to signal the error to the calling code
            raise

    def _fetch_token_data(self, context: Optional[Mapping[str, Any]]) -> Iterable[dict]:
        """Fetch historical data for a specific token."""
        self.logger.info(f"Fetching token for context: {context}")

        # Get initial page token
        next_page_token = self.get_next_page_token(None, None, context)
        self.logger.info(f"Fetching `next_page_token`: {next_page_token}")

        # If no initial token, we can't proceed
        if next_page_token is None:
            self.logger.info("No initial token - no data to fetch")
            return

        decorated_request = self.request_decorator(self._request)

        iteration_count = 0
        max_iterations = 100  # Safety limit

        while next_page_token and iteration_count < max_iterations:
            iteration_count += 1

            # Prepare the request
            prepared_request = self.prepare_request(context, next_page_token)
            prepared_request.headers.update(self.get_request_headers())
            self.logger.info(f"Making request to: {prepared_request.url}")

            # Make the API request
            response = decorated_request(prepared_request, context)
            self.logger.info(f"Response status: {response.status_code}")

            # Parse the response
            records = list(self.parse_response(response, next_page_token))
            self.logger.info(f"Parsed {len(records)} records from response")

            # Process each record
            for record in records:
                record_with_context = record if context is None else {**record, **context}
                yield record_with_context

            # Store previous token for comparison
            previous_token = copy.deepcopy(next_page_token)

            # Get the next page token
            next_page_token = self.get_next_page_token(response, previous_token, context)

            # Check for infinite loop
            if next_page_token == previous_token:
                self.logger.error("Token did not change - breaking to avoid infinite loop")
                break

            # Apply rate limiting for free API
            if next_page_token and self.config["api_url"] != "https://pro-api.coingecko.com/api/v3":
                sleep_time = self.config.get("wait_time_between_requests", 1)
                self.logger.info(f"Sleeping for {sleep_time} seconds...")
                time.sleep(sleep_time)

        if iteration_count >= max_iterations:
            self.logger.warning(f"Reached maximum iterations ({max_iterations}) - stopping")

    def post_process(self, row: dict, context: Optional[Mapping[str, Any]] = None) -> dict:
        """Process row data after retrieval for hourly data.

        Simplified compared to the daily version since we're already getting
        processed data from the market_chart endpoint.
        """
        # The data is already mostly processed in parse_response
        # Just return the row as is or add any additional processing needed
        return row

    schema = th.PropertiesList(
        th.Property("timestamp", th.IntegerType, required=True),
        th.Property("token", th.StringType, required=True),
        th.Property("price_usd", th.NumberType),
        th.Property("market_cap_usd", th.NumberType),
        th.Property("total_volume_usd", th.NumberType),
    ).to_dict()
