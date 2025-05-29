# tap_coingecko/streams/categories.py

import time
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, List

import backoff
import requests
from singer_sdk import typing as th
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError
from singer_sdk.streams import RESTStream

from tap_coingecko.streams.utils import API_HEADERS, ApiType


class CoinCategoriesStream(RESTStream):
    """
    RESTStream for fetching CoinGecko coin categories.
    Follows the structure of CoingeckoDailyStream (base.py) where applicable.
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
        """Return the base URL for API requests. (Copied from base.py)"""
        api_url_config = self.config.get("api_url")
        if api_url_config == ApiType.PRO.value:
            return ApiType.PRO.value
        elif api_url_config == ApiType.FREE.value:
            return ApiType.FREE.value
        else:
            self.logger.error(f"Invalid 'api_url' in config: '{api_url_config}'. Expected one of: {[e.value for e in ApiType]}.")
            raise ValueError(f"Invalid API URL: {api_url_config}. ")


    @property
    def path(self) -> str:
        """Return the API endpoint path for the current token. (Adapted from base.py)"""
        if not self.current_token:
            self.logger.error(f"[{self.name}] 'current_token' accessed before being set.")
            raise ValueError("No token has been set for the stream.")
        return f"/coins/{self.current_token}"

    def get_request_headers(self) -> Dict[str, str]:
        """Return API request headers. (Copied from base.py)"""
        headers: Dict[str, str] = {}
        api_url_config = self.config.get("api_url")
        api_key_value = self.config.get("api_key")

        header_name_for_key = API_HEADERS.get(str(api_url_config))

        if header_name_for_key and api_key_value:
            headers[header_name_for_key] = str(api_key_value)
        elif api_url_config == ApiType.PRO.value and not api_key_value:
            self.logger.warning(
                f"[{self.name}] Pro API URL ('{api_url_config}') configured, but API key is missing or empty. "
                f"Authentication will likely fail."
            )
        return headers

    def request_decorator(self, func: Callable) -> Callable:
        """Retry logic for API requests. (Copied from base.py, enhanced on_giveup)"""
        return backoff.on_exception(
            backoff.expo,
            (RetriableAPIError, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError),
            max_tries=8,
            factor=3,
            on_giveup=lambda details: self.logger.error(
                f"[{self.name}] Request failed after {details['tries']} tries "
                f"for {details['args'][0].url if details.get('args') and hasattr(details['args'][0], 'url') else 'N/A'}: {details.get('exception')}"
            )
        )(func)

    def get_url_params(
        self, context: Optional[Mapping[str, Any]], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Generate URL parameters for API requests."""
        # This endpoint /coins/{id} for categories does not need pagination parameters.
        # It can take optional boolean flags to reduce data, but for simplicity and
        # to match your local script (which sends no params), we return an empty dict.
        return {}


    def parse_response(self, response: requests.Response) -> Iterable[Dict]:
        """Parse API response. (Specific to /coins/{id} endpoint)"""
        self.logger.debug(f"[{self.name}] Parsing response for token '{self.current_token}'. Status: {response.status_code}")
        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError as e:
            self.logger.error(f"[{self.name}] Error decoding JSON for token '{self.current_token}' from URL {response.url}")
            self.logger.error(f"[{self.name}] Response text (first 500 chars): {response.text[:500]}")
            raise FatalAPIError(f"Failed to decode JSON response from {response.url}: {e}") from e

        record_coin_id = data.get("id")
        if not record_coin_id and self.current_token:
            self.logger.warning(f"[{self.name}] 'id' field missing in response for token '{self.current_token}'. Using contextual token ID.")
            record_coin_id = self.current_token
        
        if not record_coin_id:
            self.logger.error(f"[{self.name}] Could not determine coin_id for record. URL: {response.url}. Data snippet: {str(data)[:200]}")
            return

        yield {
            "coin_id": record_coin_id,
            "name": data.get("name"),
            "symbol": data.get("symbol"),
            "categories": data.get("categories", []),
        }

    def request_records(self, context: Optional[Mapping[str, Any]]) -> Iterable[Dict]:
        """Fetch records for all configured tokens. (Adapted from base.py)"""
        tokens_to_sync: List[str] = self.config.get("token", [])
        self.logger.info(f"[{self.name}] Starting request_records for tokens: {tokens_to_sync}")

        if not tokens_to_sync:
            self.logger.warning(f"[{self.name}] No tokens configured. Stream will be empty.")
            return

        for token_id in tokens_to_sync:
            self.current_token = token_id
            token_context = {"token": token_id}
            self.logger.info(f"[{self.name}] Processing token: {self.current_token}")

            try:
                prepared_request = self.prepare_request(
                    context=token_context,
                    next_page_token=None
                )

                auth_headers = self.get_request_headers()
                if auth_headers:
                    prepared_request.headers.update(auth_headers)
                
                # --- CORRECTED LOGGING LINE ---
                # The PreparedRequest object's __str__ or __repr__ usually shows URL and method.
                # To explicitly show URL and headers without relying on a .params attribute:
                url_params_for_log = self.get_url_params(context=token_context, next_page_token=None) # Get what would be params
                full_url_for_log = prepared_request.url
                if url_params_for_log: # If get_url_params returned anything, show it
                     # Note: requests library typically merges params into the URL in PreparedRequest
                     # or handles them separately. For logging, showing what get_url_params *would* send is useful.
                     # However, the actual prepared_request.url might already have them if they were simple.
                     # For this stream, get_url_params returns {}, so url_params_for_log will be empty.
                     pass # No explicit params to log for this stream as get_url_params is {}

                self.logger.info(
                    f"[{self.name}] Requesting for {self.current_token}: "
                    f"URL={full_url_for_log}, Headers={prepared_request.headers}"
                )
                # --- END CORRECTED LOGGING LINE ---


                response = self._request(prepared_request, token_context)

                for record in self.parse_response(response):
                    yield record

            except RetriableAPIError as e:
                self.logger.error(f"[{self.name}] API error for token '{self.current_token}' (likely after retries): {e}. Skipping token.", exc_info=True)
            except FatalAPIError as e:
                self.logger.error(f"[{self.name}] Fatal API error for token '{self.current_token}': {e}. Skipping token.", exc_info=True)
            except requests.exceptions.HTTPError as e:
                error_content = e.response.text[:1000] if e.response else "N/A"
                response_headers_str = str(e.response.headers) if e.response else "N/A"
                status_code_str = e.response.status_code if e.response else "N/A"
                request_url_str = e.request.url if e.request else "N/A"
                self.logger.error(
                    f"[{self.name}] HTTPError for token '{self.current_token}'. "
                    f"URL: {request_url_str}, Status: {status_code_str}. "
                    f"Response Headers: {response_headers_str}. Response Body: {error_content}",
                    exc_info=True
                )
            except Exception as e:
                self.logger.error(f"[{self.name}] Unexpected error processing token '{self.current_token}': {e}. Skipping token.", exc_info=True)


            api_url_config = self.config.get("api_url")
            if api_url_config and api_url_config != ApiType.PRO.value:
                wait_time = self.config.get("wait_time_between_requests", 0)
                if wait_time > 0:
                    self.logger.debug(f"[{self.name}] (Non-Pro API) Waiting {wait_time}s after processing token '{self.current_token}'.")
                    time.sleep(wait_time)

        self.logger.info(f"[{self.name}] Finished request_records.")