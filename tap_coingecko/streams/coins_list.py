"""Stream for extracting coin list data from CoinGecko API."""

from typing import Any, Dict, Mapping, Optional

from singer_sdk import typing as th
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
            th.ObjectType(additional_properties=th.StringType),
            description="Platform-specific contract addresses for the coin.",
        ),
    ).to_dict()

    @property
    def url_base(self) -> str:
        """Get the base URL for CoinGecko API requests."""
        match self.config["api_url"]:
            case ApiType.PRO.value:
                return ApiType.PRO.value
            case ApiType.FREE.value:
                return ApiType.FREE.value
            case _:
                raise ValueError(f"Invalid API URL: {self.config['api_url']}. ")

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

    @property
    def http_headers(self) -> Dict[str, str]:
        """Return HTTP headers for requests.

        Overrides the default headers to include authentication.
        """
        headers = super().http_headers.copy()

        header_key = API_HEADERS.get(self.config["api_url"])
        if not header_key:
            raise ValueError(f"Invalid API URL: {self.config['api_url']}. ")
        if not self.config.get("api_key"):
            raise ValueError("API key is required for authenticated requests.")

        self.logger.info(f"Using API key for {self.config['api_url']} with header '{header_key}'.")
        headers[header_key] = self.config["api_key"]

        return headers
