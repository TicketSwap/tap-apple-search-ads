"""AppleSearchAds tap class."""

from __future__ import annotations

from datetime import datetime, timezone

from singer_sdk import Tap
from singer_sdk.typing import (
    DateType,
    PropertiesList,
    Property,
    StringType,
    ArrayType,
)  # JSON schema typing helpers

from tap_apple_search_ads import streams


class TapAppleSearchAds(Tap):
    """AppleSearchAds tap class."""

    name = "tap-apple-search-ads"

    config_jsonschema = PropertiesList(
        Property(
            "client_id",
            StringType,
            secret=True,  # Flag config as protected.
            description="The client id to authenticate against the API service",
        ),
        Property(
            "client_secret",
            StringType,
            secret=True,  # Flag config as protected.
            description="The client secret to authenticate against the API service",
        ),
        Property(
            "org_id",
            StringType,
            description="The organisation that you want to sync. Superseded by `org_ids` if that is set.",
        ),
        Property(
            "org_ids",
            ArrayType(StringType),
            description="The organisations that you want to sync. The client_ids and client_secrets must be mapped to "
            "`TAP_APPLE_SEARCH_ADS_CLIENT_ID__<org_id>` and `TAP_APPLE_SEARCH_ADS_CLIENT_SECRET__<org_id>` "
            "environment variables respectively. If you are just syncing one organisation, you can use the standard "
            "env variables or the config values.",
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
            description=("The granularity of reporting streams. " "One of HOURLY, DAILY, WEEKLY, MONTHLY."),
            allowed_values=["HOURLY", "DAILY", "WEEKLY", "MONTHLY"],
        ),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        """Initialize the tap."""
        super().__init__(*args, **kwargs)
        if not (
            (self.config.get("org_id") and self.config.get("client_id") and self.config.get("client_secret"))
            or self.config.get("org_ids")
        ):
            msg = "You must provide either `org_id` or `org_ids` in the config."
            raise ValueError(msg)

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
