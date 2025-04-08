"""Stream classes for tap-coingecko."""

from tap_coingecko.streams.base import CoingeckoDailyStream
from tap_coingecko.streams.hourly import CoingeckoHourlyStream

__all__ = ["CoingeckoDailyStream", "CoingeckoHourlyStream"]
