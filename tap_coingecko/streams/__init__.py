"""Stream classes for tap-coingecko."""

from tap_coingecko.streams.base import CoingeckoDailyStream
from tap_coingecko.streams.categories import CoinCategoriesStream
from tap_coingecko.streams.coins_list import CoinListStream
from tap_coingecko.streams.hourly import CoingeckoHourlyStream

__all__ = [
    "CoingeckoDailyStream",
    "CoingeckoHourlyStream",
    "CoinCategoriesStream",
    "CoinListStream",
]
