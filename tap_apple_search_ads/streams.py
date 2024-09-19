"""Stream type classes for tap-apple-search-ads."""

from __future__ import annotations

import typing as t
from datetime import datetime, timezone

from tap_apple_search_ads.client import AppleSearchAdsStream

from .schemas import campaigns_schema, reports_schema

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record

_TToken = t.TypeVar("_TToken")


class CampaignsStream(AppleSearchAdsStream):
    """Define custom stream."""

    name = "campaigns"
    path = "/campaigns"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema = campaigns_schema


class ReportStream(AppleSearchAdsStream):
    """Base class for report streams.

    For now report streams only return totals and not grouped by.
    """

    rest_method = "POST"
    records_jsonpath = "$.data.reportingDataResponse.row[*]"

    @property
    def schema(self) -> dict:
        """Return schema with primary key added."""
        schema = reports_schema
        schema["properties"][self.primary_keys[0]] = {
            "type": ["integer", "null"],
        }
        return schema

    def prepare_request_payload(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: _TToken | None,  # noqa: ARG002
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.
        """
        return {
            "startTime": self.config.get("start_date", "2016-01-01"),
            "endTime": self.config.get(
                "start_date",
                datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"),
            ),
            "selector": {
                "orderBy": [{"field": self.primary_keys[0], "sortOrder": "ASCENDING"}],
                "pagination": {"offset": 0, "limit": 1000},
            },
            "timeZone": "UTC",
            "returnRecordsWithNoMetrics": True,
            "returnRowTotals": True,
            "returnGrandTotals": True,
        }

    def post_process(
        self,
        row: Record,
        context: Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            The resulting record dict, or `None` if the record should be excluded.
        """
        new_row = dict(row["total"].items())
        new_row[self.primary_keys[0]] = row["metadata"][self.primary_keys[0]]
        return new_row


class CampaignReportsStream(ReportStream):
    """Campaign reports stream."""

    path = "/reports/campaigns"
    primary_keys: t.ClassVar[list[str]] = [
        "campaignId",
    ]  # make sure this is just one key for report streams.
    name = "campaign_reports"
