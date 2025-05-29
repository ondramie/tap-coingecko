import time
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, List

import backoff
import requests 
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError 

from tap_coingecko.streams.utils import API_HEADERS, ApiType

class CoinCategoriesStream(RESTStream):
    """
    Stream for fetching CoinGecko coin categories.

    For each token specified in the tap's configuration, this stream fetches
    its ID, name, symbol, and an array of category names using the /coins/{id} endpoint.
    """

    name = "coin_categories"
    primary_keys = ["coin_id"]
    replication_method = "FULL_TABLE"  
    current_token: Optional[str] = None
   
    schema = th.PropertiesList(
        th.Property("coin_id", th.StringType, required=True,
                    description="CoinGecko ID of the coin (e.g., 'bitcoin')."),
        th.Property("name", th.StringType,
                    description="Common name of the coin (e.g., 'Bitcoin')."),
        th.Property("symbol", th.StringType,
                    description="Symbol of the coin (e.g., 'btc')."),
        th.Property("categories", th.ArrayType(th.StringType),
                    description="Array of category names assigned to the coin.")
    ).to_dict()

    @property
    def url_base(self) -> str:
        """Return the base URL for API requests based on configuration."""
        api_url_config = self.config.get("api_url")
        if api_url_config == ApiType.PRO.value:
            return ApiType.PRO.value
        elif api_url_config == ApiType.FREE.value:
            return ApiType.FREE.value
        else:
            
            self.logger.warning(f"Unknown api_url '{api_url_config}', defaulting to FREE. Known values are: {[e.value for e in ApiType]}")
        
            return ApiType.FREE.value 

    @property
    def path(self) -> str:
        """Return the API endpoint path for the current token."""
        if not self.current_token:
            # This should ideally not happen if request_records sets current_token before path is needed
            raise ValueError(
                f"[{self.name}] 'current_token' has not been set before accessing the path property."
            )
        return f"/coins/{self.current_token}"

    def get_request_headers(self) -> Dict[str, str]:
        """Return API request headers based on the API type and key."""
        headers = {}

        api_url_config = self.config.get("api_url")
        api_key_value = self.config.get("api_key")
        
        header_name_for_key = API_HEADERS.get(str(api_url_config)) 

        if header_name_for_key and api_key_value:
            headers[header_name_for_key] = str(api_key_value) 
        elif api_url_config == ApiType.PRO.value and not (header_name_for_key and api_key_value) :
            self.logger.warning(
                f"[{self.name}] Pro API URL configured but API key or header name is missing. "
                f"Header Name Found: {header_name_for_key}, API Key Present: {bool(api_key_value)}"
            )
        return headers

    def get_url_params(
        self, context: Optional[Mapping[str, Any]], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of parameters for the URL query string."""
        params: Dict[str, Any] = {
            "localization": "false",
            "tickers": "false",
            "market_data": "false",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        }
        return params

    def parse_response(self, response: requests.Response) -> Iterable[Dict]:
        """Parse the API response and yield a single record."""
        self.logger.debug(f"[{self.name}] Parsing response for token '{self.current_token}'")
        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError as e:
            self.logger.error(f"[{self.name}] Error decoding JSON for token '{self.current_token}': {e}")
            self.logger.error(f"[{self.name}] Response text: {response.text[:500]}...") # Log snippet of text
            # Depending on strictness, either raise or yield nothing
            raise FatalAPIError(f"Failed to decode JSON response: {e}") from e

        coin_id = data.get("id")
        if not coin_id and self.current_token: 
            self.logger.warning(f"[{self.name}] 'id' field missing in response for token '{self.current_token}'. Using contextual token ID.")
            coin_id = self.current_token
        elif not coin_id and not self.current_token:
             self.logger.error(f"[{self.name}] 'id' field missing in response and no current_token context.")
             return


        yield {
            "coin_id": coin_id,
            "name": data.get("name"),
            "symbol": data.get("symbol"),
            "categories": data.get("categories", []), 
        }

    def request_decorator(self, func: Callable) -> Callable:
        """Decorate the _request method with backoff logic."""
        return backoff.on_exception(
            backoff.expo,
            (RetriableAPIError, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError), # Added ConnectionError
            max_tries=7, 
            factor=3,
            logger=self.logger,
            on_giveup=lambda e: self.logger.error(f"[{self.name}] Request failed after multiple retries: {e}")
        )(func)


    def request_records(self, context: Optional[Mapping[str, Any]]) -> Iterable[Dict]:
        """Fetch category data for all configured tokens."""
        self.logger.info(f"[{self.name}] Starting sync for coin categories.")

        configured_tokens: List[str] = self.config.get("token", [])
        if not configured_tokens:
            self.logger.warning(
                f"[{self.name}] No tokens configured in 'token' list. Stream will be empty."
            )
            return

        for token_id in configured_tokens:
            self.current_token = token_id
            token_specific_context = self.build_stream_context(context={"token": token_id})

            self.logger.info(f"[{self.name}] Processing token '{self.current_token}'")

            try:
                prepared_request = self.prepare_request(
                    context=token_specific_context,
                    next_page_token=None 
                )

                auth_headers = self.get_request_headers()
                if auth_headers: # Only update if specific auth headers are returned
                    prepared_request.headers.update(auth_headers)
                else:
                    if self.config.get("api_url") == ApiType.PRO.value:
                        self.logger.warning(f"[{self.name}] Pro API is configured, but no auth headers were generated by get_request_headers(). Check API key and API_HEADERS mapping.")


                self.logger.info(
                    f"[{self.name}] Request details for token '{self.current_token}':\n"
                    f"URL: {prepared_request.url}\n"
                    f"Headers: {prepared_request.headers}\n"
                    f"Params: {prepared_request.params}"
                )

                response = self._request(
                    prepared_request=prepared_request,
                    context=token_specific_context
                )

                for record in self.parse_response(response):
                    yield record

            except FatalAPIError as e: 
                self.logger.error(
                    f"[{self.name}] Fatal API error processing token '{self.current_token}': {e}. Skipping token.",
                    exc_info=True
                )
            except RetriableAPIError as e:
                self.logger.error(
                    f"[{self.name}] Retriable API error (final attempt failed) for token '{self.current_token}': {e}. Skipping token.",
                     exc_info=True
                )
            except requests.exceptions.HTTPError as e:
                error_content = "N/A"
                response_headers_str = "N/A"
                if e.response is not None:
                    error_content = e.response.text[:1000] # Limit log size
                    response_headers_str = str(e.response.headers)
                self.logger.error(
                    f"[{self.name}] HTTPError for token '{self.current_token}'. "
                    f"Status: {e.response.status_code if e.response is not None else 'N/A'}. "
                    f"Response Headers: {response_headers_str}. "
                    f"Response Body: {error_content}",
                    exc_info=True
                )
                if e.response is not None and e.response.status_code == 404:
                     self.logger.warning(
                        f"[{self.name}] Token ID '{self.current_token}' not found (404). Skipping."
                     )
            except Exception as e: 
                self.logger.error(
                    f"[{self.name}] Unexpected error processing token '{self.current_token}': {e}. "
                    "Skipping token.",
                    exc_info=True
                )

            if self.config.get("api_url") == ApiType.FREE.value:
                wait_time = self.config.get("wait_time_between_requests", 5) 
                if wait_time > 0:
                    self.logger.debug(
                        f"[{self.name}] (Free API) Waiting {wait_time}s "
                        f"after processing token '{self.current_token}'."
                    )
                    time.sleep(wait_time)

        self.logger.info(f"[{self.name}] Finished syncing coin categories.")