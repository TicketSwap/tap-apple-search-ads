"""AppleSearchAds tap class."""

from __future__ import annotations

from datetime import datetime, timezone

from singer_sdk import Tap
from singer_sdk.typing import (
    DateType,
    PropertiesList,
    Property,
    StringType,
)  # JSON schema typing helpers

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
        Property(
            "start_date",
            DateType,
            description="Start date for reporting streams, format in YYYY-MM-DD.",
        ),
        Property(
            "end_date",
            DateType,
            description="End date for reporting streams, format in YYYY-MM-DD.",
            default=datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"),
        ),
        Property(
            "report_granularity",
            StringType,
            description=(
                "The granularity of reporting streams. "
                "One of HOURLY, DAILY, WEEKLY, MONTHLY."
            ),
            allowed_values=["HOURLY", "DAILY", "WEEKLY", "MONTHLY"],
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.AppleSearchAdsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.CampaignsStream(self),
            streams.CampaignReportsStream(self),
            streams.CampaignGranularReportsStream(self),
        ]


if __name__ == "__main__":
    TapAppleSearchAds.cli()
