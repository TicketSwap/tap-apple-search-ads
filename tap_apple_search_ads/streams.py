"""Stream type classes for tap-apple-search-ads."""

from __future__ import annotations

import typing as t
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_apple_search_ads.client import PAGE_LIMIT, AppleSearchAdsStream

from .schemas import campaigns_schema, reports_schema

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context, Record

_TToken = t.TypeVar("_TToken")


class GranularityConfig(NamedTuple):
    """Configuration for granularity settings."""

    days: int
    min_interval: int
    max_interval: int


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
    ) -> dict:
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
                "pagination": {"offset": next_page_token, "limit": PAGE_LIMIT},
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

    # Granularity configuration mapping
    GRANULARITY_CONFIGS: t.ClassVar[dict[str, GranularityConfig]] = {
        "HOURLY": GranularityConfig(days=30, min_interval=0, max_interval=7),
        "DAILY": GranularityConfig(days=90, min_interval=0, max_interval=90),
        "WEEKLY": GranularityConfig(days=365 * 2, min_interval=14, max_interval=365),
        "MONTHLY": GranularityConfig(days=365 * 2, min_interval=31 * 3, max_interval=365 * 2),
    }

    def _get_initial_start_date(self, context: Context | None) -> datetime:
        """Get the initial start date from various sources."""
        start_date_str = (
            self.get_starting_replication_key_value(context) or self.config.get("start_date") or "1900-01-01"
        )
        return datetime.fromisoformat(start_date_str).replace(tzinfo=timezone.utc)

    def _adjust_start_date(self, start_date: datetime, config: GranularityConfig, now: datetime) -> datetime:
        """Adjust start date based on granularity constraints."""
        # Check minimum start date constraint
        min_start_date = now - timedelta(days=config.days)
        if start_date < min_start_date:
            self.logger.info(
                "Start date is before minimum start date for this granularity, " "setting start date to %s",
                min_start_date.strftime("%Y-%m-%d"),
            )
            start_date = min_start_date

        # Check minimum interval constraint
        min_interval_start_date = now - timedelta(days=config.min_interval)
        if start_date > min_interval_start_date:
            self.logger.info(
                "Start date is after minimum interval date for this granularity, " "setting start date to %s",
                min_interval_start_date.strftime("%Y-%m-%d"),
            )
            start_date = min_interval_start_date

        return start_date

    def _adjust_end_date(self, end_date: datetime, start_date: datetime, config: GranularityConfig) -> datetime:
        """Adjust end date based on maximum interval constraint."""
        max_end_date = start_date + timedelta(days=config.max_interval)
        if max_end_date < end_date:
            self.logger.info(
                "End date is after maximum end date for this granularity, " "setting end date to %s",
                max_end_date.strftime("%Y-%m-%d"),
            )
            return max_end_date
        return end_date

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
        config = self.GRANULARITY_CONFIGS[granularity]

        payload["granularity"] = granularity

        now = datetime.now(tz=timezone.utc)
        start_date = self._get_initial_start_date(context)
        start_date = self._adjust_start_date(start_date, config, now)
        end_date = self._adjust_end_date(now, start_date, config)

        payload.update(
            {
                "startTime": start_date.strftime("%Y-%m-%d"),
                "endTime": end_date.strftime("%Y-%m-%d"),
                "returnRowTotals": False,
                "returnGrandTotals": False,
            }
        )

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
        "date",
    ]  # make sure the first key is the report type
    name = "campaign_granular_reports"
