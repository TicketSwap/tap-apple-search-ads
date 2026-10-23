"""REST client handling, including AppleSearchAdsStream base class."""

from __future__ import annotations

import functools
import typing as t
from typing import TYPE_CHECKING, Any

from singer_sdk.pagination import OffsetPaginator
from singer_sdk.streams import RESTStream

from tap_apple_search_ads.auth import AppleSearchAdsAuthenticator

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

PAGE_LIMIT = 1000


@functools.cache
def _get_authenticator(
    org_id: str,
    is_partitioned: bool,  # noqa: FBT001
    client_id: str | None,
    client_secret: str | None,
) -> AppleSearchAdsAuthenticator:
    return AppleSearchAdsAuthenticator(
        org_id=org_id,
        is_partitioned=is_partitioned,
        client_id=client_id,
        client_secret=client_secret,
        auth_endpoint="https://appleid.apple.com/auth/oauth2/token",
        oauth_scopes="searchadsorg",
        default_expiration=3600,
        oauth_headers={
            "Host": "appleid.apple.com",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )


class AppleSearchAdsStream(RESTStream):
    """AppleSearchAds stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$.data[*]"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://api.searchads.apple.com/api/v5"

    @property
    def authenticator(self) -> AppleSearchAdsAuthenticator:
        """Get an authenticator object.

        Returns:
            The authenticator instance for this REST stream.
        """
        return _get_authenticator(
            self.org_id,
            self.partitions is not None,
            self.config.get("client_id"),
            self.config.get("client_secret"),
        )

    @property
    def partitions(self) -> list[dict] | None:
        """Return a list of partitions, or None if the stream is not partitioned."""
        if self.config.get("org_ids") is None:
            return None
        return [{"org_id": org_id} for org_id in self.config["org_ids"]]

    def get_records(self, context: Context) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        if self.partitions is None:
            self.org_id = self.config["org_id"]
        else:
            self.org_id = context["org_id"]
        yield from self.request_records(context)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        headers["X-AP-Context"] = f"orgId={self.org_id}"
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_new_paginator(self) -> OffsetPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return OffsetPaginator(0, PAGE_LIMIT)

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        params["limit"] = PAGE_LIMIT
        if next_page_token:
            params["offset"] = next_page_token
        if self.replication_key:
            params["field"] = "asc"
            params["order_by"] = self.replication_key
        return params
