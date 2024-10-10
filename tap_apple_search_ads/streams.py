"""Stream type classes for tap-apple-search-ads."""

from __future__ import annotations

import typing as t
from datetime import datetime, timedelta, timezone

from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_apple_search_ads.client import AppleSearchAdsStream

from .schemas import campaigns_schema, reports_schema

if t.TYPE_CHECKING:
    import requests
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
        context: Context | None,
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
            "endTime": self.config.get("end_date"),
            "selector": {
                "orderBy": [{"field": self.primary_keys[0], "sortOrder": "ASCENDING"}],
                "pagination": {"offset": 0, "limit": 1000},
            },
            "timeZone": "UTC",
            "returnRecordsWithNoMetrics": True,
            "returnRowTotals": True,
            "returnGrandTotals": True,
        }

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: A raw :class:`requests.Response`

        Yields:
            One item for every item found in the response.
        """
        for record in extract_jsonpath(self.records_jsonpath, input=response.json()):
            record["total"]["metadata"] = record["metadata"]
            yield record["total"]

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
        row[self.primary_keys[0]] = row["metadata"][self.primary_keys[0]]
        return row


class GranularReportsStream(ReportStream):
    """Base class for report streams.

    This stream returns granular results for reports set by `report_granularity`
    """

    replication_key = "date"

    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: _TToken | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.
        """
        payload = super().prepare_request_payload(context, next_page_token)
        granularity = self.config["report_granularity"]
        payload["granularity"] = granularity
        match granularity:
            case "HOURLY":
                days, min_interval, max_interval = 30, 0, 7
            case "DAILY":
                days, min_interval, max_interval = 90, 0, 90
            case "WEEKLY":
                days, min_interval, max_interval = 365 * 2, 14, 365
            case "MONTHLY":
                days, min_interval, max_interval = 365 * 2, 31 * 3, 365 * 2
        now = datetime.now(tz=timezone.utc)
        min_start_date = now - timedelta(days=days)
        start_date = datetime.fromisoformat(
            self.get_starting_replication_key_value(
                context,
            )
            or self.config.get("start_date")
            or "1900-01-01",
        )
        start_date = start_date.replace(tzinfo=timezone.utc)
        if start_date < min_start_date:
            start_date = min_start_date
            self.logger.info(
                (
                    "Start date is before minimum start date for this "
                    "granularity, setting start date to %s"
                ),
                start_date.strftime("%Y-%m-%d"),
            )
        start_date = start_date.replace(tzinfo=timezone.utc)
        min_interval_start_date = now - timedelta(days=min_interval)
        if start_date > min_interval_start_date:
            start_date = min_interval_start_date
            self.logger.info(
                (
                    "Start date is after minimum interval date for this "
                    "granularity, setting start date to %s"
                ),
                start_date.strftime("%Y-%m-%d"),
            )
        end_date = now
        max_end_date = start_date + timedelta(max_interval)
        if max_end_date < end_date:
            end_date = max_end_date
            self.logger.info(
                (
                    "End date is after maximum end date for this "
                    "granularity, setting end date to %s"
                ),
                end_date.strftime("%Y-%m-%d"),
            )
        payload["endTime"] = end_date.strftime("%Y-%m-%d")
        payload["startTime"] = start_date.strftime("%Y-%m-%d")
        payload["returnRowTotals"] = False
        payload["returnGrandTotals"] = False
        return payload

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: A raw :class:`requests.Response`

        Yields:
            One item for every item found in the response.
        """
        for record in extract_jsonpath(self.records_jsonpath, input=response.json()):
            for granular_record in record["granularity"]:
                granular_record["metadata"] = record["metadata"]
                yield granular_record


class CampaignReportsStream(ReportStream):
    """Campaign reports stream."""

    path = "/reports/campaigns"
    primary_keys: t.ClassVar[list[str]] = [
        "campaignId",
    ]  # make sure this is just one key for report streams.
    name = "campaign_reports"


class CampaignGranularReportsStream(GranularReportsStream):
    """Campaign granular reports stream."""

    path = "/reports/campaigns"
    primary_keys: t.ClassVar[list[str]] = [
        "campaignId",
    ]  # make sure this is just one key for report streams.
    name = "campaign_granular_reports"
