# tap_coingecko/streams/categories.py

import time # For rate limiting
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, List # Ensure List is imported

import backoff
import requests
from singer_sdk import typing as th
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError # Ensure FatalAPIError is available
from singer_sdk.streams import RESTStream

# Assuming utils.py is in the same directory or accessible in python path
from tap_coingecko.streams.utils import API_HEADERS, ApiType


class CoinCategoriesStream(RESTStream):
    """
    RESTStream for fetching CoinGecko coin categories.
    Follows the structure of CoingeckoDailyStream (base.py) where applicable.
    """

    name = "coin_categories"
    # primary_keys differ from base.py
    primary_keys = ["coin_id"]
    # replication_method differs from base.py; this stream is a full snapshot per token.
    replication_method = "FULL_TABLE"
    # No replication_key, is_sorted, state_partitioning_keys for FULL_TABLE in this context.

    current_token: Optional[str] = None # Instance variable to store current token being processed

    # Schema definition for coin categories
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

    # Methods copied or adapted from CoingeckoDailyStream (base.py)

    @property
    def url_base(self) -> str:
        """Return the base URL for API requests. (Copied from base.py)"""
        api_url_config = self.config.get("api_url")
        if api_url_config == ApiType.PRO.value:
            return ApiType.PRO.value
        elif api_url_config == ApiType.FREE.value:
            return ApiType.FREE.value
        else:
            # Behavior for unexpected api_url. tap.py should enforce valid values.
            # CoingeckoDailyStream raises ValueError, which is stricter.
            self.logger.error(f"Invalid 'api_url' in config: '{api_url_config}'. Expected one of: {[e.value for e in ApiType]}.")
            raise ValueError(f"Invalid API URL: {api_url_config}. ")


    @property
    def path(self) -> str:
        """Return the API endpoint path for the current token. (Adapted from base.py)"""
        if not self.current_token: # Check current_token directly
            self.logger.error(f"[{self.name}] 'current_token' accessed before being set.")
            raise ValueError("No token has been set for the stream.")
        # path differs from base.py: no '/history'
        return f"/coins/{self.current_token}"

    def get_request_headers(self) -> Dict[str, str]:
        """Return API request headers. (Copied from base.py)"""
        headers: Dict[str, str] = {} # Ensure headers is initialized
        api_url_config = self.config.get("api_url")
        # api_key should be present if required=True in tap.py, otherwise it could be None.
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
        """Retry logic for API requests. (Copied from base.py)"""
        # Note: base.py does not pass logger=self.logger to backoff.on_exception.
        # The SDK's default behavior for backoff usually picks up the stream's logger.
        return backoff.on_exception(
            backoff.expo,
            (RetriableAPIError, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError),
            max_tries=8,
            factor=3,
            on_giveup=lambda details: self.logger.error( # Added on_giveup for better logging
                f"[{self.name}] Request failed after {details['tries']} tries "
                f"for {details['args'][0].url if details.get('args') and hasattr(details['args'][0], 'url') else 'N/A'}: {details.get('exception')}"
            )
        )(func)

    def get_url_params(
        self, context: Optional[Mapping[str, Any]], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Generate URL parameters for API requests. (Adapted for this endpoint)"""
        # This endpoint (/coins/{id}) doesn't use date-based pagination like /history.
        # It takes boolean flags to include/exclude data sections.
        # To minimize data transfer if only categories are needed:
        params: Dict[str, Any] = {
            "localization": "false",
            "tickers": "false",
            "market_data": "false",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        }
        # If you discover that categories are only returned when other flags are true, adjust accordingly.
        # Your local script implies no parameters are strictly needed for fetching categories.
        # Returning {} as per your original categories.py and local script is also an option if the above params cause issues.
        # For strict adherence to minimal necessary params like base.py, `{"localization": "false"}` could be a starting point.
        # Let's use {} to match original behavior more closely for this specific endpoint.
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
        # Fallback to current_token if 'id' is missing in response, common for robustness
        if not record_coin_id and self.current_token:
            self.logger.warning(f"[{self.name}] 'id' field missing in response for token '{self.current_token}'. Using contextual token ID.")
            record_coin_id = self.current_token
        
        if not record_coin_id:
            self.logger.error(f"[{self.name}] Could not determine coin_id for record. URL: {response.url}. Data snippet: {str(data)[:200]}")
            return # Skip this problematic record

        yield {
            "coin_id": record_coin_id,
            "name": data.get("name"),
            "symbol": data.get("symbol"),
            "categories": data.get("categories", []), # Default to empty list if key is absent
        }

    def request_records(self, context: Optional[Mapping[str, Any]]) -> Iterable[Dict]:
        """Fetch records for all configured tokens. (Adapted from base.py)"""
        # This stream fetches data per token without explicit pagination in the API call itself.
        # The overall structure of looping through tokens is similar to base.py's request_records.

        tokens_to_sync: List[str] = self.config.get("token", [])
        self.logger.info(f"[{self.name}] Starting request_records for tokens: {tokens_to_sync}")

        if not tokens_to_sync:
            self.logger.warning(f"[{self.name}] No tokens configured. Stream will be empty.")
            return

        # Get the decorated _request method.
        # The SDK applies the decorator defined on the class to self._request automatically.
        # So, calling self._request will use the decorated version.

        for token_id in tokens_to_sync:
            self.current_token = token_id
            # Context for this specific token. base.py sets `token_context = token, {"token": token}` which is a typo.
            # It should be `self.current_token = token` and `token_context = {"token": token}`.
            token_context = {"token": token_id}
            self.logger.info(f"[{self.name}] Processing token: {self.current_token}")

            try:
                # `next_page_token=None` as this endpoint isn't paginated in the traditional sense.
                prepared_request = self.prepare_request(
                    context=token_context,
                    next_page_token=None
                )

                # CRITICAL: Explicitly update headers, as in base.py's _fetch_token_data
                auth_headers = self.get_request_headers()
                if auth_headers: # Only update if auth_headers is not empty
                    prepared_request.headers.update(auth_headers)
                
                self.logger.info(
                    f"[{self.name}] Requesting for {self.current_token}: "
                    f"URL={prepared_request.url}, Headers={prepared_request.headers}, Params={prepared_request.params}"
                )

                # self._request handles the actual call and retries (due to request_decorator)
                response = self._request(prepared_request, token_context)

                for record in self.parse_response(response):
                    yield record

            # Handling exceptions that might occur after retries or are not retriable
            except RetriableAPIError as e: # Should be caught by backoff, but log if it propagates
                self.logger.error(f"[{self.name}] API error for token '{self.current_token}' (likely after retries): {e}. Skipping token.", exc_info=True)
            except FatalAPIError as e: # For non-retriable API errors (e.g. 401, 403 if not made retriable)
                self.logger.error(f"[{self.name}] Fatal API error for token '{self.current_token}': {e}. Skipping token.", exc_info=True)
            except requests.exceptions.HTTPError as e: # Fallback if SDK doesn't wrap it
                # This might be redundant if SDK's _request -> validate_response handles all HTTP errors.
                # Log detailed HTTP error information
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
            except Exception as e: # Catch-all for other unexpected errors
                self.logger.error(f"[{self.name}] Unexpected error processing token '{self.current_token}': {e}. Skipping token.", exc_info=True)


            # Rate limiting for FREE API (copied from base.py, adjusted self.name and logic)
            api_url_config = self.config.get("api_url")
            # Check if 'api_url' key exists AND if it's not the PRO URL.
            # Assumes anything not PRO might need rate limiting as per original logic.
            if api_url_config and api_url_config != ApiType.PRO.value:
                wait_time = self.config.get("wait_time_between_requests", 0) # Default to 0 if not set
                if wait_time > 0:
                    self.logger.debug(f"[{self.name}] (Non-Pro API) Waiting {wait_time}s after processing token '{self.current_token}'.")
                    time.sleep(wait_time)

        self.logger.info(f"[{self.name}] Finished request_records.")