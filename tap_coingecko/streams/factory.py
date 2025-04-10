"""Module for Token-specific Stream Factory."""

from typing import List, Type

from singer_sdk import Stream

from tap_coingecko.streams.hourly import CoingeckoHourlyStream


class TokenStreamFactory:
    """Factory class to generate token-specific streams."""

    @classmethod
    def create_stream_class(cls, token: str) -> Type[CoingeckoHourlyStream]:
        """Create a token-specific hourly stream class.

        Args:
        token: The token name to create a stream for.

        Returns:
            A new stream class for the specific token.
        """
        # Create a new class that inherits from CoingeckoHourlyStream
        class_name = f"Coingecko{token.title()}HourlyStream"

        # Create the new class with a specific name
        token_specific_class = type(
            class_name,
            (CoingeckoHourlyStream,),
            {
                "name": f"{token.lower()}_price_hourly",
                "current_token": token.lower(),
                "__doc__": f"Stream for hourly {token} data from CoinGecko.",
            },
        )

        return token_specific_class

    @classmethod
    def generate_stream_classes(cls, tokens: List[str]) -> List[Type[Stream]]:
        """Generate all stream classes for the given tokens.

        Args:
            tokens: The list of tokens to generate streams for.

        Returns:
            A list of all generated stream classes.
        """
        return [cls.create_stream_class(token) for token in tokens]
