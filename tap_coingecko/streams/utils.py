from enum import Enum


class ApiType(Enum):
    PRO = "https://pro-api.coingecko.com/api/v3"
    FREE = "https://api.coingecko.com/api/v3"


API_HEADERS = {
    "https://pro-api.coingecko.com/api/v3": "x-cg-pro-api-key",
    "https://api.coingecko.com/api/v3": "x-cg-demo-api-key",
}
