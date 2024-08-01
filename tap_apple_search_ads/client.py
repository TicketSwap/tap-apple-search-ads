"""REST client handling, including AppleSearchAdsStream base class."""

from __future__ import annotations

import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Iterable

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseOffsetPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream

from tap_apple_search_ads.auth import AppleSearchAdsAuthenticator

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]

class AppleSearchAdsPaginator(BaseOffsetPaginator):
    def __init__(self, start_value: int, page_size: int, *args: Any, **kwargs: Any) -> None:
        super().__init__(start_value, page_size, *args, **kwargs)

    def has_more(self, response: requests.Response) -> bool:
        pagination = response.json().get("pagination", {})
        total_results = pagination.get("totalResults", 0)
        start_index = pagination.get("startIndex", 0)
        items_per_page = pagination.get("itemsPerPage", 0)
        return start_index + items_per_page < total_results

class AppleSearchAdsStream(RESTStream):
    """AppleSearchAds stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$.data[*]"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://api.searchads.apple.com/api/v5"

    @cached_property
    def authenticator(self) -> _Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return AppleSearchAdsAuthenticator.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        headers["X-AP-Context"] = f"orgId={self.config.get('org_id')}"
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_new_paginator(self) -> BaseOffsetPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return AppleSearchAdsPaginator(0, 1000)

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
        if next_page_token:
            params["offset"] = next_page_token
        if self.replication_key:
            params["field"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
