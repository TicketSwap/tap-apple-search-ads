"""Schemas for streams."""

from singer_sdk.typing import (  # JSON Schema typing helpers
    ArrayType,
    BooleanType,
    DateTimeType,
    DateType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

campaigns_schema = PropertiesList(
    Property("id", IntegerType),
    Property("orgId", IntegerType),
    Property("name", StringType),
    Property("name", StringType),
    Property(
        "budgetAmount",
        ObjectType(
            Property("amount", StringType),
            Property("currency", StringType),
        ),
    ),
    Property(
        "dailyBudgetAmount",
        ObjectType(
            Property("amount", StringType),
            Property("currency", StringType),
        ),
    ),
    Property("adamId", IntegerType),
    Property("paymentModel", StringType),
    Property(
        "locInvoiceDetails",
        ObjectType(
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

reports_schema = PropertiesList(
    Property("date", DateType),
    Property("impressions", IntegerType),
    Property("taps", IntegerType),
    Property("ttr", NumberType),
    Property(
        "avgCPT",
        ObjectType(
            Property("amount", StringType),
            Property("currency", StringType),
        ),
    ),
    Property(
        "avgCPM",
        ObjectType(
            Property("amount", StringType),
            Property("currency", StringType),
        ),
    ),
    Property(
        "localSpend",
        ObjectType(
            Property("amount", StringType),
            Property("currency", StringType),
        ),
    ),
    Property("totalInstalls", IntegerType),
    Property("totalNewDownloads", IntegerType),
    Property("totalRedownloads", IntegerType),
    Property("viewInstalls", IntegerType),
    Property("tapInstalls", IntegerType),
    Property("tapNewDownloads", IntegerType),
    Property("tapRedownloads", IntegerType),
    Property("viewNewDownloads", IntegerType),
    Property("viewRedownloads", IntegerType),
    Property(
        "totalAvgCPI",
        ObjectType(
            Property("amount", StringType),
            Property("currency", StringType),
        ),
    ),
    Property("totalInstallRate", NumberType),
    Property(
        "tapInstallCPI",
        ObjectType(
            Property("amount", StringType),
            Property("currency", StringType),
        ),
    ),
    Property("tapInstallRate", NumberType),
).to_dict()
