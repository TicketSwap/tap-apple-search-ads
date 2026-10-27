"""Tests standard tap features using the built-in SDK tests library."""

import datetime as dt

from singer_sdk.testing import get_tap_test_class

from tap_apple_search_ads.tap import TapAppleSearchAds

SAMPLE_CONFIG = {
    "org_ids": ["1227480"],
    "report_granularity": "DAILY",
    "start_date": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d"),
}


# Run standard built-in tap tests from the SDK:
TestTapAppleSearchAds = get_tap_test_class(
    tap_class=TapAppleSearchAds,
    config=SAMPLE_CONFIG,
)
