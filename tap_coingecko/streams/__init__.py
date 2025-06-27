"""Stream classes for tap-coingecko."""

from tap_coingecko.streams.asset_profile import AssetProfileStream
from tap_coingecko.streams.base import CoingeckoDailyStream
from tap_coingecko.streams.coins_list import CoinListStream
from tap_coingecko.streams.discovery import NewlyListedStream, TopMoversStream
from tap_coingecko.streams.hourly import CoingeckoHourlyStream
from tap_coingecko.streams.market_intelligence import DerivativesSentimentStream, TrendingStream

__all__ = [
    "CoingeckoDailyStream",
    "CoingeckoHourlyStream",
    "CoinListStream",
    "AssetProfileStream",
    "TrendingStream",
    "DerivativesSentimentStream",
    "TopMoversStream",
    "NewlyListedStream",
]
