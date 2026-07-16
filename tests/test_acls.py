"""Tests for the acls stream."""

from singer_sdk.pagination import SinglePagePaginator

from tap_apple_search_ads.streams import AclsStream
from tap_apple_search_ads.tap import TapAppleSearchAds

CONFIG_SINGLE = {
    "org_id": "111",
    "client_id": "x",
    "client_secret": "y",
    "start_date": "2020-01-01",
}
CONFIG_MULTI = {
    "org_ids": ["111", "222"],
    "start_date": "2020-01-01",
}


def _acls_stream(config: dict) -> AclsStream:
    tap = TapAppleSearchAds(config=config, validate_config=False)
    return AclsStream(tap)


def test_acls_uses_single_page_paginator() -> None:
    """`/acls` is not paginated, so it must not use the offset paginator.

    The offset paginator loops forever here because the endpoint ignores
    offset/limit and returns the full list on every request.
    """
    stream = _acls_stream(CONFIG_SINGLE)
    assert isinstance(stream.get_new_paginator(), SinglePagePaginator)


def test_acls_partitions_single_org() -> None:
    """A single-org config yields exactly one partition (synced once)."""
    stream = _acls_stream(CONFIG_SINGLE)
    assert stream.partitions == [{"org_id": "111"}]


def test_acls_partitions_multi_org_uses_first_org() -> None:
    """`/acls` returns every accessible org, so one partition is enough."""
    stream = _acls_stream(CONFIG_MULTI)
    assert stream.partitions == [{"org_id": "111"}]
