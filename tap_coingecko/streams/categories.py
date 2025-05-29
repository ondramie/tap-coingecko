"""Module for CoinGecko Coin Categories Stream."""

import time
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, List

import backoff
import requests
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import RetriableAPIError

from tap_coingecko.streams.utils import API_HEADERS, ApiType

class CoinCategoriesStream(RESTStream):
    """Stream for fetching CoinGecko coin categories.

    For each token specified in the tap's configuration, this stream fetches 
    its ID, name, symbol, and an array of category names.
    """

    name = "coin_categories"
    primary_keys = ["coin_id"]
    replication_method = "FULL_TABLE"
    current_token: Optional[str] = None

    @property
    def url_base(self) -> str:
        """Return the base URL for API requests."""
        match self.config["api_url"]:
            case ApiType.PRO.value:
                return ApiType.PRO.value
            case ApiType.FREE.value:
                return ApiType.FREE.value
            case _:
                raise ValueError(
                    f"Invalid API URL in config: '{self.config['api_url']}'. "
                    f"Must match one of the defined ApiType values."
                )

    @property
    def path(self) -> str:
        """Return the API endpoint path for the current token."""
        if not self.current_token:
            raise ValueError(
                f"{self.name} stream: 'current_token' has not been set before "
                "accessing the path property."
            )
        return f"/coins/{self.current_token}" 

    def request_decorator(self, func: Callable) -> Callable:
        """Retry logic for API requests, identical to CoingeckoDailyStream."""
        return backoff.on_exception(
            backoff.expo,
            (RetriableAPIError, requests.exceptions.ReadTimeout),
            max_tries=8,
            factor=3,
            logger=self.logger
        )(func)

    def get_request_headers(self) -> Dict[str, str]:
        """Return API request headers based on the API type and key.
        """
        header_name = API_HEADERS.get(self.config["api_url"])
        
        api_key_value = self.config.get("api_key")

        if header_name and api_key_value:
            return {header_name: api_key_value}
        
        return {} 

    def get_url_params(
        self, context: Optional[Mapping[str, Any]], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of parameters for the URL query string.
        """
        return {}

    def parse_response(self, response: requests.Response) -> Iterable[Dict]:
        """Parse the API response."""
        self.logger.debug(f"{self.name} stream: Parsing response for token '{self.current_token}'")
        data = response.json()

        record_coin_id = data.get("id", self.current_token)
        name = data.get("name") 
        symbol = data.get("symbol")
        category_names = data.get("categories", []) 

        yield {
            "coin_id": record_coin_id,
            "name": name,
            "symbol": symbol,
            "categories": category_names,
        }

    def request_records(self, context: Optional[Mapping[str, Any]]) -> Iterable[Dict]:
        """Fetch category data for all configured tokens."""
        self.logger.info(f"{self.name} stream: Starting sync for coin categories.")

        configured_tokens: List[str] = self.config.get("token", [])
        if not configured_tokens:
            self.logger.warning(
                f"{self.name} stream: No tokens configured. Stream will be empty."
            )
            return

        for token_id in configured_tokens:
            self.current_token = token_id 
            token_specific_context = {"token": token_id} 
            self.logger.info(f"{self.name} stream: Processing token '{self.current_token}'")
            
            try:
                prepared_request = self.prepare_request(
                    context=token_specific_context, 
                    next_page_token=None
                )
                response = self._request(prepared_request, context=token_specific_context)

                for record in self.parse_response(response):
                    yield record 
            
            except RetriableAPIError as e:
                self.logger.error(
                    f"{self.name} stream: Retriable API error for token '{self.current_token}' "
                    f"after all retries: {e}. Skipping token."
                )
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                     self.logger.warning(
                        f"{self.name} stream: Token ID '{self.current_token}' not found (404). Skipping."
                     )
                else:
                     self.logger.error(
                        f"{self.name} stream: HTTP error for token '{self.current_token}': {e}. Skipping token."
                     )
            except Exception as e:
                self.logger.error(
                    f"{self.name} stream: Unexpected error processing token '{self.current_token}': {e}. "
                    "Skipping token.",
                    exc_info=True
                )

            if self.config.get("api_url") == ApiType.FREE.value:
                wait_time = self.config.get("wait_time_between_requests", 5)
                self.logger.debug(
                    f"{self.name} stream (Free API): Waiting {wait_time}s "
                    f"after processing token '{self.current_token}'."
                )
                time.sleep(wait_time)
        
        self.logger.info(f"{self.name} stream: Finished syncing coin categories.")

    schema = th.PropertiesList(
        th.Property("coin_id", th.StringType, required=True,
                    description="CoinGecko ID of the coin (e.g., 'bitcoin')."),
        th.Property("name", th.StringType, 
                    description="Common name of the coin (e.g., 'Bitcoin')."), # Added name
        th.Property("symbol", th.StringType,
                    description="Symbol of the coin (e.g., 'btc')."),
        th.Property("categories", th.ArrayType(th.StringType),
                    description="Array of category names assigned to the coin.")
    ).to_dict()