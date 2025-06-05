"""Stream for extracting coin list data from CoinGecko API."""

import time
from typing import Any, Callable, Dict, Iterable, Mapping, Optional

import backoff
import requests
from singer_sdk import typing as th
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.streams import RESTStream

from tap_coingecko.streams.utils import API_HEADERS, ApiType


class CoinListStream(RESTStream):
    """Stream for retrieving full coin list from CoinGecko API."""

    name = "coin_list"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"

    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            required=True,
            description="CoinGecko ID of the coin (e.g., 'bitcoin').",
        ),
        th.Property("symbol", th.StringType, description="Symbol of the coin (e.g., 'btc')."),
        th.Property(
            "name", th.StringType, description="Common name of the coin (e.g., 'Bitcoin')."
        ),
        th.Property(
            "platforms",
            th.ObjectType(additional_properties=True),
            description="Platform-specific contract addresses for the coin.",
        ),
    ).to_dict()

    @property
    def url_base(self) -> str:
        """Get the base URL for CoinGecko API requests."""
        api_url_config = self.config.get("api_url")
        if api_url_config == ApiType.PRO.value:
            return ApiType.PRO.value
        elif api_url_config == ApiType.FREE.value:
            return ApiType.FREE.value
        else:
            expected_values = [e.value for e in ApiType]
            self.logger.error(
                f"Invalid 'api_url' in config: '{api_url_config}'. "
                f"Expected one of: {expected_values}."
            )
            raise ValueError(f"Invalid API URL: {api_url_config}. ")

    @property  # type: ignore[override]
    def path(self) -> str:
        """Get the API path for the coins list endpoint."""
        return "/coins/list"

    def get_url_params(
        self, context: Optional[Mapping[str, Any]], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["include_platform"] = "true"
        return params

    def get_request_headers(self) -> Dict[str, str]:
        """Get headers for API requests, including authentication if configured."""
        headers: Dict[str, str] = {}
        api_url_config = self.config.get("api_url")
        api_key_value = self.config.get("api_key")

        header_name_for_key = API_HEADERS.get(str(api_url_config))

        if header_name_for_key and api_key_value:
            headers[header_name_for_key] = str(api_key_value)
        elif api_url_config == ApiType.PRO.value and not api_key_value:
            self.logger.warning(
                f"[{self.name}] Pro API URL ('{api_url_config}') configured, "
                f"but API key is missing or empty. Authentication will likely fail."
            )
        return headers

    def request_decorator(self, func: Callable) -> Callable:
        """Decorate requests with retry logic using exponential backoff."""
        return backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
            ),
            max_tries=8,
            factor=3,
            on_giveup=lambda details: self.logger.error(
                f"[{self.name}] Request failed after {details['tries']} tries: "
                f"{details.get('exception')}"
            ),
        )(func)

    def get_records(self, context: Optional[Mapping[str, Any]]) -> Iterable[Dict[str, Any]]:
        """Get records from the CoinGecko API."""
        # Apply rate limiting if configured
        wait_time = self.config.get("wait_time_between_requests", 0)
        if wait_time > 0:
            self.logger.info(f"[{self.name}] Waiting {wait_time} seconds before request...")
            time.sleep(wait_time)

        # Make the request and return records
        self.logger.info(f"[{self.name}] Fetching complete list of coins from CoinGecko API")
        yield from self.request_records(context)
