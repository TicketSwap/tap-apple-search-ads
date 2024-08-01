"""Stream type classes for tap-apple-search-ads."""

from __future__ import annotations

import sys
import typing as t

from singer_sdk.typing import PropertiesList, Property, DateTimeType, StringType, BooleanType, IntegerType, ObjectType, ArrayType  # JSON Schema typing helpers

from tap_apple_search_ads.client import AppleSearchAdsStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

class CampaignsStream(AppleSearchAdsStream):
    """Define custom stream."""

    name = "campaigns"
    path = "/campaigns"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None

    schema = PropertiesList(
        Property("id", IntegerType),
        Property("orgId", IntegerType),
        Property("name", StringType),
        Property("name", StringType),
        Property("budgetAmount", ObjectType(
            Property("amount", StringType),
            Property("currency", StringType),
            ),
        ),
        Property("dailyBudgetAmount", ObjectType(
            Property("amount", StringType),
            Property("currency", StringType),
            ),
        ),
        Property("adamId", IntegerType),
        Property("paymentModel", StringType),
        Property("locInvoiceDetails", ObjectType(
            Property("clientName", StringType),
            Property("orderNumber", StringType),
            Property("buyerName", StringType),
            Property("buyerEmail", StringType),
            Property("billingContactEmail", StringType),
            ),
        ),
        Property("budgetOrders", ArrayType(IntegerType)),
        Property("startTime", DateTimeType),
        Property("endTime", DateTimeType),
        Property("status", StringType),
        Property("servingStatus", StringType),
        Property("creationTime", DateTimeType),
        Property("servingStateReasons", ArrayType(StringType)),
        Property("modificationTime", DateTimeType),
        Property("deleted", BooleanType),
        Property("sapinLawResponse", StringType),
        Property("countriesOrRegions", ArrayType(StringType)),
        Property("countryOrRegionServingStateReasons", ObjectType()),
        Property("supplySources", ArrayType(StringType)),
        Property("adChannelType", StringType),
        Property("billingEvent", StringType),
        Property("displayStatus", StringType),
    ).to_dict()