"""AppleSearchAds tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk.typing import PropertiesList, Property, StringType, DateTimeType  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_apple_search_ads import streams


class TapAppleSearchAds(Tap):
    """AppleSearchAds tap class."""

    name = "tap-apple-search-ads"

    config_jsonschema = PropertiesList(
        Property(
            "client_id",
            StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The client id to authenticate against the API service",
        ),
        Property(
            "client_secret",
            StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The client secret to authenticate against the API service",
        ),
        Property(
            "org_id",
            StringType,
            required=True,
            description="The organisation id in your apple search ads.",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.AppleSearchAdsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.CampaignsStream(self),
        ]


if __name__ == "__main__":
    TapAppleSearchAds.cli()
