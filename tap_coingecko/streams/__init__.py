"""Stream classes for tap-coingecko."""

from tap_coingecko.streams.base import CoingeckoDailyStream
from tap_coingecko.streams.hourly import CoingeckoHourlyStream
from tap_coingecko.streams.categories import CoinCategoriesStream

__all__ = ["CoingeckoDailyStream", "CoingeckoHourlyStream", "CoinCategoriesStream"]
