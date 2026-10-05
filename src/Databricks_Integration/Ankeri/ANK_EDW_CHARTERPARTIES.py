# Databricks notebook source
# DBTITLE 1,Lib
from pyspark.sql import Row, Window, functions as F, types as T
from delta.tables import DeltaTable
from pyspark.sql import Window as W
from datetime import datetime, timezone
import requests
import json
import time
import logging
import uuid
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# DBTITLE 1,Config
# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
NOTEBOOK_NAME    = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
log              = logging.getLogger(NOTEBOOK_NAME)
dbutils.widgets.text("env","dev")
log.setLevel(logging.INFO)
if not log.handlers:
    _handler     = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    log.addHandler(_handler)

env              = dbutils.widgets.get("env")
CATALOG          = f"{env}_corpdatav2"
BRONZE_DB        = f"{CATALOG}.bronze"
SILVER_DB        = f"{CATALOG}.silver"
AUDIT_CATALOG    = f"{env}_audit"
LOGS             = f"{AUDIT_CATALOG}.logs"

LOGS_TABLE       = f"{LOGS}.{NOTEBOOK_NAME}"
BRONZE_TABLE     = f"{BRONZE_DB}.ANK"

# Bronze table constants (one per API endpoint)
BRONZE_CHARTERPARTIES    = f"{BRONZE_TABLE}_charterparties"
BRONZE_OFFHIRES          = f"{BRONZE_TABLE}_offhires"
BRONZE_REVIEWS           = f"{BRONZE_TABLE}_reviews"
BRONZE_SHIPS             = f"{BRONZE_TABLE}_ships"
BRONZE_SHIPS_PROPERTIES  = f"{BRONZE_TABLE}_ships_properties"

# Silver table constants (one per normalize target)
SILVER_TABLE                   = f"{SILVER_DB}.ANK"
SILVER_SHIPS                   = f"{SILVER_TABLE}_ships"
SILVER_SHIPS_PROPERTIES        = f"{SILVER_TABLE}_ships_properties"
SILVER_CHARTERPARTIES_RATES    = f"{SILVER_TABLE}_charterparties_rates"
SILVER_CHARTERPARTIES_LUMP_SUMS = f"{SILVER_TABLE}_charterparties_lump_sums"
SILVER_CHARTERPARTIES_ATCOST   = f"{SILVER_TABLE}_charterparties_atcost"
SILVER_CHARTERPARTIES_EXTENSION = f"{SILVER_TABLE}_charterparties_extension"
SILVER_CHARTERPARTIES           = f"{SILVER_TABLE}_charterparties"
SILVER_CHARTERPARTIES_OPTION    = f"{SILVER_TABLE}_charterparties_option"
SILVER_CHARTERPARTIES_ALL       = f"{SILVER_TABLE}_charterparties_all"
SILVER_CP_BROKERAGE_COMMISSION  = f"{SILVER_TABLE}_charterparty_brokerageCommission"
SILVER_CP_DELIVERY_NOTICES      = f"{SILVER_TABLE}_charterparty_deliveryNotices"
SILVER_CP_PREMIUMS              = f"{SILVER_TABLE}_charterparty_premiums"
SILVER_CP_REDELIVERY_NOTICES    = f"{SILVER_TABLE}_charterparty_redeliveryNotices"
SILVER_OFFHIRES                 = f"{SILVER_DB}.ANK_offhires"
SILVER_OFFHIRES_ATCOST          = f"{SILVER_DB}.ANK_offhires_atCost"
SILVER_OFFHIRES_LUMPSUMS        = f"{SILVER_DB}.ANK_offhire_lumpSums"
SILVER_OFFHIRES_RATES           = f"{SILVER_DB}.ANK_offhire_rates"
SILVER_REVIEWS                  = f"{SILVER_DB}.ANK_reviews"
SILVER_OFFHIRES_BUNKER          = f"{SILVER_DB}.ANK_offhire_bunker"

# Static tables
PROPERTY_MAP = f"{env}_corpdata.bronze.ank_ship_property_map"
DOCUMENT_LOOKUP = f"{env}_corpdata.bronze.ank_extension_document_lookup"
VESSEL_TABLE = f"{env}_corpdata.gold.dimvesselss"

BASE_APIM        = "https://apim.api-dev.seaspancorp.com/ankeri-clone"
ENDPOINTS        = {
    "/charter-parties" : "_charterparties",         # Endpoint, Table Suffix
    "/charter-parties/offhires" : "_offhires",
    "/charter-parties/reviews" : "_reviews",
    "/ships" : "_ships",
}
# Ships properties — handled separately via concurrent fetch after silver ships is populated
SHIPS_PROPERTIES_ENDPOINT = "/ships/{}/properties"
SHIPS_PROPERTIES_SUFFIX   = "_ships_properties"

APIM_KEY     = dbutils.secrets.get("azure-scope", "adb-ankeri-apim")
headers      = {"apikey": APIM_KEY, "Content-Type": "application/json"}
batch_run_id = str(uuid.uuid4())

# ---------------------------------------------------------------------------
# Config summary
# ---------------------------------------------------------------------------
log.info(f"env          : {env}")
log.info(f"Bronze table : {BRONZE_TABLE}")
log.info(f"Logs table   : {LOGS_TABLE}")
log.info(f"APIM base    : {BASE_APIM}")
log.info(f"Endpoints    : {len(ENDPOINTS)} active — {', '.join(ENDPOINTS.keys())}")
log.info(f"batch_run_id : {batch_run_id}")

# COMMAND ----------

# DBTITLE 1,Schemas
audit_schema = T.StructType([
    T.StructField("EventID", T.StringType(), True),
    T.StructField("batch_run_id", T.StringType(), True),
    T.StructField("api_type", T.StringType(), True),
    T.StructField("http_method", T.StringType(), True),
    T.StructField("api_url", T.StringType(), True),
    T.StructField("input_value", T.StringType(), True),
    T.StructField("request_body", T.StringType(), True),
    T.StructField("attempt_number", T.IntegerType(), True),
    T.StructField("request_timestamp", T.TimestampType(), True),
    T.StructField("response_code", T.IntegerType(), True),
    T.StructField("response_body", T.StringType(), True),
    T.StructField("duration_ms", T.LongType(), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("error_message", T.StringType(), True),
    T.StructField("execution_context", T.StringType(), True),
])

bronze_schema = T.StructType([
    T.StructField("Raw_data", T.StringType(), True),
    T.StructField("Api_metadata", T.StringType(), True),
    T.StructField("_is_normalized", T.BooleanType(), True),
    T.StructField("_Batch_run_id", T.StringType(), True),
    T.StructField("_ingestion_ts", T.TimestampType(), True),
])

properties_bronze_schema = T.StructType([
    T.StructField("ImoNr", T.StringType(), True),
    T.StructField("Raw_data", T.StringType(), True),
    T.StructField("Api_metadata", T.StringType(), True),
    T.StructField("_is_normalized", T.BooleanType(), True),
    T.StructField("_Batch_run_id", T.StringType(), True),
    T.StructField("_ingestion_ts", T.TimestampType(), True),
])

# ---------------------------------------------------------------------------
# Schema: ANK /ships page
# ---------------------------------------------------------------------------
_ships_page_schema = T.StructType([
    T.StructField("count", T.IntegerType(), True),
    T.StructField("ships", T.ArrayType(T.StructType([
        T.StructField("name",                T.StringType(),  True),
        T.StructField("imoNr",               T.LongType(),    True),
        T.StructField("provisionalId",       T.StringType(),  True),
        T.StructField("profileCompleteness", T.IntegerType(), True),
        T.StructField("lastUpdated",         T.StringType(),  True),
        T.StructField("isReporting",         T.BooleanType(), True),
        T.StructField("own",                 T.BooleanType(), True),
        T.StructField("charter",             T.BooleanType(), True),
        T.StructField("shipType",            T.StringType(),  True),
    ])), True),
])

# ---------------------------------------------------------------------------
# Schema: ANK /charter-parties — rates normalization
# ---------------------------------------------------------------------------
_cp_rates_schema = T.StructType([
    T.StructField("contracts", T.ArrayType(T.StructType([
        T.StructField("contractId",         T.LongType(),   True),
        T.StructField("rate_review_period", T.StringType(), True),
        T.StructField("rates", T.ArrayType(T.StructType([
            T.StructField("rate",       T.StringType(),  True),
            T.StructField("special",    T.BooleanType(), True),
            T.StructField("currency",   T.StringType(),  True),
            T.StructField("rate_uuid",  T.StringType(),  True),
            T.StructField("valid_from", T.StringType(),  True),
            T.StructField("lump_sums", T.ArrayType(T.StructType([
                T.StructField("type",          T.StringType(), True),
                T.StructField("ls_unit",       T.StringType(), True),
                T.StructField("ls_amount",     T.StringType(), True),
                T.StructField("ls_comment",    T.StringType(), True),
                T.StructField("ls_currency",   T.StringType(), True),
                T.StructField("rate_number",   T.StringType(), True),
                T.StructField("lump_sum_uuid", T.StringType(), True),
            ])), True),
        ])), True),
    ])), True),
])

# ---------------------------------------------------------------------------
# Schema: ANK /charter-parties — lump_sums normalization
# ---------------------------------------------------------------------------
_cp_lump_sums_schema = T.StructType([
    T.StructField("contracts", T.ArrayType(T.StructType([
        T.StructField("contractId", T.LongType(), True),
        T.StructField("rates", T.ArrayType(T.StructType([
            T.StructField("lump_sums", T.ArrayType(T.StructType([
                T.StructField("type",          T.StringType(), True),
                T.StructField("ls_unit",       T.StringType(), True),
                T.StructField("ls_amount",     T.StringType(), True),
                T.StructField("ls_currency",   T.StringType(), True),
                T.StructField("rate_number",   T.StringType(), True),
                T.StructField("lump_sum_uuid", T.StringType(), True),
            ])), True),
        ])), True),
    ])), True),
])

# ---------------------------------------------------------------------------
# Schema: ANK /charter-parties — atCost normalization
# ---------------------------------------------------------------------------
_cp_at_cost_schema = T.StructType([
    T.StructField("contracts", T.ArrayType(T.StructType([
        T.StructField("contractId", T.LongType(), True),
        T.StructField("atCost", T.StringType(), True),
    ])), True),
])

# ---------------------------------------------------------------------------
# Schema: ANK /charter-parties — extension normalization
# ---------------------------------------------------------------------------
_cp_charterparties_schema = T.StructType([
    T.StructField("contracts", T.ArrayType(T.StructType([
        T.StructField("contractId",              T.LongType(),    True),
        T.StructField("contractSeriesId",        T.LongType(),    True),
        T.StructField("contractType",            T.StringType(),  True),
        T.StructField("contractPeriod",          T.StringType(),  True),
        T.StructField("shipImoNr",               T.StringType(),  True),
        T.StructField("shipName",                T.StringType(),  True),
        T.StructField("charterType",             T.StringType(),  True),
        T.StructField("chartererId",             T.LongType(),    True),
        T.StructField("chartererName",           T.StringType(),  True),
        T.StructField("currentOwnerId",          T.LongType(),    True),
        T.StructField("ownerName",               T.StringType(),  True),
        T.StructField("brokerId",                T.LongType(),    True),
        T.StructField("brokerName",              T.StringType(),  True),
        T.StructField("brokerIsCompetitive",     T.BooleanType(), True),
        T.StructField("fixtureDate",             T.StringType(),  True),
        T.StructField("mainTermsDate",           T.StringType(),  True),
        T.StructField("bodApprovalDate",         T.StringType(),  True),
        T.StructField("bod_approval_date_charterer", T.StringType(), True),
        T.StructField("addendumDate",            T.StringType(),  True),
        T.StructField("addendumNumber",          T.StringType(),  True),
        T.StructField("laydaysDate",             T.StringType(),  True),
        T.StructField("cancellingDate",          T.StringType(),  True),
        T.StructField("layDateIsUtc",            T.BooleanType(), True),
        T.StructField("cancellingDateIsUtc",     T.BooleanType(), True),
        T.StructField("layDate",                 T.StringType(),  True),
        T.StructField("canDate",                 T.StringType(),  True),
        T.StructField("deliveryDate",            T.StringType(),  True),
        T.StructField("extensionDate",           T.StringType(),  True),
        T.StructField("option_type",             T.StringType(),  True),
        T.StructField("declarationDate",         T.StringType(),  True),
        T.StructField("declarationType",         T.StringType(),  True),
        T.StructField("deliveryPortArea",        T.StringType(),  True),
        T.StructField("deliveryPortPlace",       T.StringType(),  True),
        T.StructField("deliveryStatus",          T.StringType(),  True),
        T.StructField("redeliveryStartDate",     T.StringType(),  True),
        T.StructField("redeliveryEndDate",       T.StringType(),  True),
        T.StructField("redeliveryDate",          T.StringType(),  True),
        T.StructField("redeliveryRange",         T.StringType(),  True),
        T.StructField("redeliveryPortPlace",     T.StringType(),  True),
        T.StructField("redeliveryPlan",          T.StringType(),  True),
        T.StructField("redeliveryStatus",        T.StringType(),  True),
        T.StructField("charterPartyId",          T.StringType(),  True),
        T.StructField("cpDate",                  T.StringType(),  True),
        T.StructField("created",                 T.StringType(),  True),
        T.StructField("lastModified",            T.StringType(),  True),
        T.StructField("fromDate",                T.StringType(),  True),
        T.StructField("toDate",                  T.StringType(),  True),
        T.StructField("period",                  T.StringType(),  True),
        T.StructField("fromDateTimezone",        T.StringType(),  True),
        T.StructField("toDateTimezone",          T.StringType(),  True),
        T.StructField("fromDateIsPlanned",       T.BooleanType(), True),
        T.StructField("toDateIsPlanned",         T.BooleanType(), True),
        T.StructField("comments",                T.StringType(),  True),
        T.StructField("commentsForNextExtension", T.StringType(), True),
        T.StructField("ifrsRelevant",            T.StringType(),  True),
        T.StructField("isDryDockClause",         T.BooleanType(), True),
        T.StructField("dryDockClauseComment",    T.StringType(),  True),
        T.StructField("bunker_clause",           T.BooleanType(), True),
        T.StructField("bunker_clause_comment",   T.StringType(),  True),
        T.StructField("war_insurance_clause",    T.BooleanType(), True),
        T.StructField("war_insurance_arranged_by", T.StringType(), True),
        T.StructField("war_insurance_paid_by",   T.StringType(),  True),
        T.StructField("ciiClause",               T.BooleanType(), True),
        T.StructField("ciiComment",              T.StringType(),  True),
        T.StructField("ciiRatingAtDelivery",     T.StringType(),  True),
        T.StructField("ciiRatingAtRedelivery",   T.StringType(),  True),
        T.StructField("etsClause",               T.BooleanType(), True),
        T.StructField("etsComment",              T.StringType(),  True),
        T.StructField("etsNotifyChartererWithin", T.StringType(), True),
        T.StructField("etsOptionToSettleEuaInCash", T.StringType(), True),
        T.StructField("etsOptionToSettleEuaInCashPeriod", T.StringType(), True),
        T.StructField("etsPaymentFulfilment",    T.StringType(),  True),
        T.StructField("etsReportingPeriod",      T.StringType(),  True),
        T.StructField("etsReportingPeriodSplit", T.StringType(),  True),
        T.StructField("etsReportingRedelivery",  T.StringType(),  True),
        T.StructField("etsReportingRedeliveryDays", T.StringType(), True),
        T.StructField("etsReportingRedeliveryMonths", T.StringType(), True),
        T.StructField("etsSettlementRedelivery", T.StringType(),  True),
        T.StructField("etsSettlementRedeliveryDays", T.StringType(), True),
        T.StructField("etsSettlementRedeliveryMonths", T.StringType(), True),
        T.StructField("etsTransferOfEuaToOwner", T.StringType(),  True),
        T.StructField("etsTransferOfEuaToOwnerDate", T.StringType(), True),
        T.StructField("etsTransferOfEuaToOwnerNumeric", T.StringType(), True),
        T.StructField("hasPersistedLumpsums",    T.BooleanType(), True),
        T.StructField("hireInvoiceRequired",     T.BooleanType(), True),
        T.StructField("optionToAddOffhireDays",  T.StringType(),  True),
        T.StructField("optionToAddOffhireDaysComment", T.StringType(), True),
        T.StructField("fixerEmail",              T.StringType(),  True),
        T.StructField("postfixerEmail",          T.StringType(),  True),
        T.StructField("rate_type",               T.StringType(),  True),
        T.StructField("rate_floor",              T.StringType(),  True),
        T.StructField("rate_ceiling",            T.StringType(),  True),
        T.StructField("rate_discount_premium",   T.DoubleType(),  True),
        T.StructField("rate_review_period",      T.StringType(),  True),
        T.StructField("rate_comment",            T.StringType(),  True),
        T.StructField("addressCommission", T.StringType(), True),
        T.StructField("rateBillingPeriods", T.ArrayType(T.StructType([
            T.StructField("rbp",     T.StringType(), True),
            T.StructField("comment", T.StringType(), True),
        ])), True),
        T.StructField("external_fields", T.StructType([
            T.StructField("account_id",                  T.StringType(), True),
            T.StructField("crmd_commission",             T.StringType(), True),
            T.StructField("crmd_earliesttcenddateupdate", T.StringType(), True),
            T.StructField("crmd_finalizedrate",          T.DoubleType(), True),
            T.StructField("crmd_hull",                   T.StringType(), True),
            T.StructField("crmd_name",                   T.StringType(), True),
            T.StructField("fixture_id",                  T.StringType(), True),
            T.StructField("rate_id",                     T.StringType(), True),
            T.StructField("statuscode",                  T.LongType(),   True),
            T.StructField("statuscodeName",              T.StringType(), True),
        ]), True),
    ])), True),
])

# ---------------------------------------------------------------------------
# Schema: brokerageCommission (nested array within each contract)
# ---------------------------------------------------------------------------
_cp_brokerage_commission_schema = T.StructType([
    T.StructField("contracts", T.ArrayType(T.StructType([
        T.StructField("contractId", T.StringType()),
        T.StructField("brokerageCommission", T.ArrayType(T.StructType([
            T.StructField("amount", T.StringType()),
            T.StructField("end_date", T.StringType()),
            T.StructField("broker_id", T.StringType()),
            T.StructField("absolute_payment", T.StringType()),
            T.StructField("ceiling_currency", T.StringType()),
            T.StructField("deduct_from_hire", T.StringType()),
            T.StructField("absolute_payment_currency", T.StringType()),
            T.StructField("absolute_payment_rate_billing_period", T.StringType()),
        ]))),
    ])), True),
])

# ---------------------------------------------------------------------------
# Schema: deliveryNotices + redeliveryNotices (same inner struct, one schema)
# ---------------------------------------------------------------------------
_cp_notices_schema = T.StructType([
    T.StructField("contracts", T.ArrayType(T.StructType([
        T.StructField("contractId", T.StringType()),
        T.StructField("deliveryNotices", T.ArrayType(T.StructType([
            T.StructField("nr_days", T.StringType()),
            T.StructField("date_to_tender", T.StringType()),
            T.StructField("date_to_tender_formatted", T.StringType()),
            T.StructField("is_definite", T.StringType()),
        ]))),
        T.StructField("redeliveryNotices", T.ArrayType(T.StructType([
            T.StructField("nr_days", T.StringType()),
            T.StructField("date_to_tender", T.StringType()),
            T.StructField("date_to_tender_formatted", T.StringType()),
            T.StructField("is_definite", T.StringType()),
        ]))),
    ])), True),
])

# ---------------------------------------------------------------------------
# Schema: premiums (nested array within each contract)
# ---------------------------------------------------------------------------
_cp_premiums_schema = T.StructType([
    T.StructField("contracts", T.ArrayType(T.StructType([
        T.StructField("contractId", T.StringType()),
        T.StructField("premiums", T.ArrayType(T.StructType([
            T.StructField("start", T.StringType()),
            T.StructField("end", T.StringType()),
            T.StructField("type", T.StringType()),
            T.StructField("uuid", T.StringType()),
            T.StructField("amount", T.StringType()),
            T.StructField("comment", T.StringType()),
        ]))),
    ])), True),
])

# ---------------------------------------------------------------------------
# Schema: offhires (flat array from BRONZE_OFFHIRES — OPENJSON(RawData))
# ---------------------------------------------------------------------------
_offhires_schema = T.ArrayType(T.StructType([
    T.StructField("actual_settlement_amount", T.StringType()),
    T.StructField("add_offhire_duration_to_charter_period", T.StringType()),
    T.StructField("add_offhire_duration_to_charter_period_comment", T.StringType()),
    T.StructField("address_commission", T.StringType()),
    T.StructField("address_commission_amount", T.StringType()),
    T.StructField("broker", T.StructType([
        T.StructField("full_name", T.StringType()),
        T.StructField("id", T.StringType()),
        T.StructField("initials", T.StringType()),
        T.StructField("primary_color", T.StringType()),
        T.StructField("short_name", T.StringType()),
    ])),
    T.StructField("brokerage_commission", T.StringType()),
    T.StructField("brokerage_commission_amount_total", T.StringType()),
    T.StructField("bunker_amounts", T.StructType([
        T.StructField("lng", T.StringType()),
        T.StructField("vlsfo", T.StringType()),
        T.StructField("ulsmgo", T.StringType()),
        T.StructField("lsmgo", T.StringType()),
        T.StructField("ulsfo", T.StringType()),
        T.StructField("hfo", T.StringType()),
        T.StructField("hsfo", T.StringType()),
    ])),
    T.StructField("bunker_value", T.StringType()),
    T.StructField("charter_direction", T.StringType()),
    T.StructField("charter_party_id", T.StringType()),
    T.StructField("charterer", T.StructType([
        T.StructField("full_name", T.StringType()),
        T.StructField("id", T.StringType()),
        T.StructField("initials", T.StringType()),
        T.StructField("primary_color", T.StringType()),
        T.StructField("short_name", T.StringType()),
    ])),
    T.StructField("cii_clause", T.StringType()),
    T.StructField("cii_clause_rating_at_delivery", T.StringType()),
    T.StructField("cii_clause_rating_at_redelivery", T.StringType()),
    T.StructField("comments", T.StringType()),
    T.StructField("contract_date", T.StringType()),
    T.StructField("contract_id", T.StringType()),
    T.StructField("contract_id_internal", T.StringType()),
    T.StructField("contract_status_ready", T.StringType()),
    T.StructField("contract_type", T.StringType()),
    T.StructField("contract_type_text", T.StringType()),
    T.StructField("currency", T.StringType()),
    T.StructField("damage_reports_count", T.StringType()),
    T.StructField("duration", T.StringType()),
    T.StructField("ets_clause", T.StringType()),
    T.StructField("ets_comment", T.StringType()),
    T.StructField("ets_notify_charterer_within", T.StringType()),
    T.StructField("ets_option_to_settle_eua_in_cash", T.StringType()),
    T.StructField("ets_option_to_settle_eua_in_cash_period", T.StringType()),
    T.StructField("ets_payment_fulfilment", T.StringType()),
    T.StructField("ets_reporting_period", T.StringType()),
    T.StructField("ets_reporting_period_split", T.StringType()),
    T.StructField("ets_settlement_redelivery", T.StringType()),
    T.StructField("ets_settlement_redelivery_days", T.StringType()),
    T.StructField("ets_transfer_of_eua_to_owner", T.StringType()),
    T.StructField("ets_transfer_of_eua_to_owner_date", T.StringType()),
    T.StructField("ets_transfer_of_eua_to_owner_numeric", T.StringType()),
    T.StructField("file_count", T.StringType()),
    T.StructField("fixer", T.StructType([
        T.StructField("initials", T.StringType()),
    ])),
    T.StructField("from_date", T.StringType()),
    T.StructField("from_date_is_planned", T.StringType()),
    T.StructField("from_date_timezone", T.StringType()),
    T.StructField("fuel_eu_allowed_to_bank", T.StringType()),
    T.StructField("fuel_eu_allowed_to_bank_notify_by", T.StringType()),
    T.StructField("fuel_eu_allowed_to_bank_reporting_period", T.StringType()),
    T.StructField("fuel_eu_allowed_to_borrow", T.StringType()),
    T.StructField("fuel_eu_allowed_to_borrow_notify_by", T.StringType()),
    T.StructField("fuel_eu_allowed_to_borrow_reimburse_charterers_for_paid_surcharge_days_after", T.StringType()),
    T.StructField("fuel_eu_allowed_to_borrow_reporting_period", T.StringType()),
    T.StructField("fuel_eu_allowed_to_pool", T.StringType()),
    T.StructField("fuel_eu_allowed_to_pool_notify_by", T.StringType()),
    T.StructField("fuel_eu_allowed_to_pool_reimburse_charterers_for_paid_surcharge_days_after", T.StringType()),
    T.StructField("fuel_eu_allowed_to_pool_reporting_period", T.StringType()),
    T.StructField("fuel_eu_clause", T.StringType()),
    T.StructField("fuel_eu_comment", T.StringType()),
    T.StructField("fuel_eu_compliance_balance_at_delivery", T.StringType()),
    T.StructField("fuel_eu_compliance_balance_at_delivery_date", T.StringType()),
    T.StructField("fuel_eu_compliance_balance_at_previous_reporting_period", T.StringType()),
    T.StructField("fuel_eu_compliance_balance_at_previous_reporting_period_year", T.StringType()),
    T.StructField("fuel_eu_delivery_agreement", T.StringType()),
    T.StructField("fuel_eu_delivery_agreement_negative_compliance_balance_currency", T.StringType()),
    T.StructField("fuel_eu_delivery_agreement_negative_compliance_balance_max", T.StringType()),
    T.StructField("fuel_eu_delivery_agreement_negative_compliance_balance_per_tonne", T.StringType()),
    T.StructField("fuel_eu_delivery_agreement_positive_compliance_balance_currency", T.StringType()),
    T.StructField("fuel_eu_delivery_agreement_positive_compliance_balance_max", T.StringType()),
    T.StructField("fuel_eu_delivery_agreement_positive_compliance_balance_per_tonne", T.StringType()),
    T.StructField("fuel_eu_notify_charterer_within", T.StringType()),
    T.StructField("fuel_eu_reference_code", T.StringType()),
    T.StructField("fuel_eu_reimburse_charterers_for_positive_compliance_balance", T.StringType()),
    T.StructField("fuel_eu_reimburse_charterers_for_positive_compliance_balance_currency", T.StringType()),
    T.StructField("fuel_eu_reimburse_charterers_for_positive_compliance_balance_max", T.StringType()),
    T.StructField("fuel_eu_reimburse_charterers_for_positive_compliance_balance_paid_by", T.StringType()),
    T.StructField("fuel_eu_reimburse_charterers_for_positive_compliance_balance_per_tonne", T.StringType()),
    T.StructField("fuel_eu_reimburse_owner_for_penalty_multiplier", T.StringType()),
    T.StructField("fuel_eu_reimburse_owner_for_penalty_multiplier_amount", T.StringType()),
    T.StructField("fuel_eu_reimburse_owner_for_penalty_multiplier_currency", T.StringType()),
    T.StructField("fuel_eu_reimburse_owner_for_penalty_multiplier_paid_by", T.StringType()),
    T.StructField("fuel_eu_reporting_period", T.StringType()),
    T.StructField("fuel_eu_surcharge_settlement", T.StringType()),
    T.StructField("hire_value", T.StringType()),
    T.StructField("is_last_in_series", T.StringType()),
    T.StructField("is_planned", T.StringType()),
    T.StructField("last_rate", T.StringType()),
    T.StructField("last_rate_currency", T.StringType()),
    T.StructField("location", T.StringType()),
    T.StructField("ls_value", T.StringType()),
    T.StructField("managed_in_pool", T.StringType()),
    T.StructField("managed_in_pool_comment", T.StringType()),
    T.StructField("offhire_date", T.StringType()),
    T.StructField("offhire_ratio", T.StringType()),
    T.StructField("offhire_uuid", T.StringType()),
    T.StructField("offhires_count", T.StringType()),
    T.StructField("offhires_duration", T.StringType()),
    T.StructField("onhire_date", T.StringType()),
    T.StructField("onhire_duration", T.StringType()),
    T.StructField("option_declaration_type", T.StringType()),
    T.StructField("option_declaration_type_text", T.StringType()),
    T.StructField("option_to_add_offhire_days", T.StringType()),
    T.StructField("option_to_add_offhire_days_comment", T.StringType()),
    T.StructField("option_type", T.StringType()),
    T.StructField("order_date", T.StringType()),
    T.StructField("owner", T.StructType([
        T.StructField("full_name", T.StringType()),
        T.StructField("id", T.StringType()),
        T.StructField("initials", T.StringType()),
        T.StructField("primary_color", T.StringType()),
        T.StructField("short_name", T.StringType()),
    ])),
    T.StructField("period", T.StringType()),
    T.StructField("postfixer", T.StructType([
        T.StructField("email", T.StringType()),
        T.StructField("enabled", T.StringType()),
        T.StructField("id", T.StringType()),
        T.StructField("initials", T.StringType()),
        T.StructField("username", T.StringType()),
    ])),
    T.StructField("rate_type", T.StringType()),
    T.StructField("rate_type_label", T.StringType()),
    T.StructField("reason", T.StringType()),
    T.StructField("redelivery_max_date", T.StringType()),
    T.StructField("redelivery_min_date", T.StringType()),
    T.StructField("redelivery_period", T.StringType()),
    T.StructField("redelivery_plan", T.StringType()),
    T.StructField("redelivery_range", T.StringType()),
    T.StructField("ship_archived", T.StringType()),
    T.StructField("ship_id", T.StringType()),
    T.StructField("ship_imo_nr", T.StringType()),
    T.StructField("ship_name", T.StringType()),
    T.StructField("ship_nominal_capacity", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("to_count", T.StringType()),
    T.StructField("to_date", T.StringType()),
    T.StructField("to_date_is_planned", T.StringType()),
    T.StructField("to_date_timezone", T.StringType()),
    T.StructField("total_value", T.StringType()),
    T.StructField("atCost", T.ArrayType(T.StructType([
        T.StructField("at_cost_type", T.StringType()),
        T.StructField("at_cost_unit", T.StringType()),
        T.StructField("at_cost_amount", T.StringType()),
        T.StructField("at_cost_comment", T.StringType()),
    ]))),
    T.StructField("bunker", T.MapType(T.StringType(), T.StructType([
        T.StructField("roe", T.StringType()),
        T.StructField("price", T.StringType()),
        T.StructField("based_on", T.StringType()),
        T.StructField("onhire_quantity", T.StringType()),
        T.StructField("offhire_quantity", T.StringType()),
        T.StructField("additional_quantity", T.StringType()),
    ]))),
    T.StructField("rates", T.ArrayType(T.StructType([
        T.StructField("rate", T.StringType()),
        T.StructField("rate_number", T.StringType()),
        T.StructField("special", T.StringType()),
        T.StructField("currency", T.StringType()),
        T.StructField("rate_uuid", T.StringType()),
        T.StructField("valid_from", T.StringType()),
        T.StructField("lump_sums", T.ArrayType(T.StructType([
            T.StructField("type", T.StringType()),
            T.StructField("ls_unit", T.StringType()),
            T.StructField("ls_amount", T.StringType()),
            T.StructField("ls_currency", T.StringType()),
            T.StructField("rate_number", T.StringType()),
            T.StructField("lump_sum_uuid", T.StringType()),
        ]))),
    ]))),
]))

# ---------------------------------------------------------------------------
# Schema: reviews (flat array from BRONZE_REVIEWS)
# ---------------------------------------------------------------------------
_reviews_schema = T.ArrayType(T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("uuidhex", T.StringType()),
    T.StructField("contract_id", T.StringType()),
    T.StructField("contract_type", T.StringType()),
    T.StructField("review_type", T.StringType()),
    T.StructField("review_type_raw", T.StringType()),
    T.StructField("contract_date", T.StringType()),
    T.StructField("ship_name", T.StringType()),
    T.StructField("ship_id", T.StringType()),
    T.StructField("ship_imo_nr", T.StringType()),
    T.StructField("number_of_changes", T.StringType()),
    T.StructField("modified_date", T.StringType()),
    T.StructField("modified_by", T.StringType()),
    T.StructField("approved_date", T.StringType()),
    T.StructField("approval_approved", T.StringType()),
    T.StructField("approved_by", T.StringType()),
    T.StructField("fixer", T.StringType()),
    T.StructField("postfixer", T.StringType()),
]))

# ---------------------------------------------------------------------------
# Schema: ships_properties (single JSON object per bronze row)
# ---------------------------------------------------------------------------
_ships_properties_schema = T.StructType([
    T.StructField("uuid", T.StringType()),
    T.StructField("lastUpdated", T.StringType()),
    T.StructField("shipImoNr", T.StringType()),
    T.StructField("properties", T.ArrayType(T.StructType([
        T.StructField("id", T.StringType()),
        T.StructField("title", T.StringType()),
        T.StructField("value", T.StringType()),
        T.StructField("isVerified", T.StringType()),
        T.StructField("lastUpdated", T.StringType()),
    ]))),
])


# COMMAND ----------

# DBTITLE 1,Helper Functions
# ---------------------------------------------------------------------------
# Helper: call_api — pure caller with retry + OData pagination
# ---------------------------------------------------------------------------
def call_api(
    method: str,
    url: str,
    headers: dict,
    input_value: str = None,
    body: dict = None,
    max_retries: int = 3,
    backoff_factor: float = 1.5,
) -> dict:
    """
    Makes the API request with retry logic and OData pagination support.
    Returns a result dict:
      - attempts : list of dicts — one entry per HTTP attempt across all pages
      - pages    : list of dicts — one entry per successful, non-empty page
      - status   : 'SUCCESS' if at least one page landed; 'FAILED' otherwise
    """
    attempts, pages = [], []
    current_url = url

    while current_url:
        page_succeeded = False
        page_resp_code, page_resp_body, page_req_ts = None, None, None

        for attempt in range(1, max_retries + 1):
            req_ts = datetime.now(timezone.utc)
            err_msg, resp_code, resp_body = None, None, None

            try:
                resp = requests.request(method, current_url, headers=headers, json=body, timeout=90)
                duration_ms = int((datetime.now(timezone.utc) - req_ts).total_seconds() * 1000)
                resp_code, resp_body = resp.status_code, resp.text

                if 200 <= resp_code <= 204:
                    atm_status = "SUCCESS"
                    page_succeeded = True
                    page_resp_code, page_resp_body, page_req_ts = resp_code, resp_body, req_ts
                elif attempt < max_retries:
                    atm_status = "RETRY"
                    err_msg = f"HTTP {resp_code}"
                else:
                    atm_status = "FAILED"
                    err_msg = f"HTTP {resp_code}"

            except Exception as exc:
                duration_ms = int((datetime.now(timezone.utc) - req_ts).total_seconds() * 1000)
                err_msg = str(exc)
                atm_status = "RETRY" if attempt < max_retries else "FAILED"

            attempts.append({
                "EventID":           str(uuid.uuid4()),
                "batch_run_id":      batch_run_id,
                "http_method":       method,
                "api_url":           current_url,
                "input_value":       input_value,
                "request_body":      json.dumps(body) if body else None,
                "attempt_number":    attempt,
                "request_timestamp": req_ts,
                "response_code":     resp_code,
                "response_body":     resp_body,
                "duration_ms":       duration_ms,
                "status":            atm_status,
                "error_message":     err_msg,
            })

            if page_succeeded:
                break
            if attempt < max_retries:
                time.sleep(backoff_factor ** attempt)

        if not page_succeeded:
            break

        # Skip empty responses (OData value:[])
        _is_empty = False
        try:
            _parsed = json.loads(page_resp_body)
            _is_empty = (isinstance(_parsed, list) and not _parsed) or \
                        (isinstance(_parsed, dict) and _parsed.get("value") == [])
        except Exception:
            pass

        if not _is_empty:
            pages.append({
                "body":              page_resp_body,
                "http_status":       page_resp_code,
                "request_timestamp": page_req_ts,
            })

        # OData next-link pagination
        try:
            _next = json.loads(page_resp_body)
            current_url = _next.get("@odata.nextLink") or _next.get("nextLink")
        except Exception:
            current_url = None

    return {
        "method":       method,
        "url":          url,
        "batch_run_id": batch_run_id,
        "input_value":  input_value,
        "attempts":     attempts,
        "pages":        pages,
        "status":       "SUCCESS" if pages else "FAILED",
    }


# ---------------------------------------------------------------------------
# Helper: log_audit — writes one row per attempt to the audit logs table
# ---------------------------------------------------------------------------
def log_audit(results, api_type: str) -> None:
    """
    Writes one row per attempt to the audit logs table.
    Accepts a single call_api result dict or a list of result dicts.
    """
    if isinstance(results, dict):
        results = [results]
    rows = [
        Row(
            EventID=a["EventID"],
            batch_run_id=a["batch_run_id"],
            api_type=api_type,
            http_method=a["http_method"],
            api_url=a["api_url"],
            input_value=a["input_value"],
            request_body=a["request_body"],
            attempt_number=a["attempt_number"],
            request_timestamp=a["request_timestamp"],
            response_code=a["response_code"],
            response_body=a["response_body"],
            duration_ms=a["duration_ms"],
            status=a["status"],
            error_message=a["error_message"],
            execution_context=NOTEBOOK_NAME,
        )
        for result in results
        for a in result["attempts"]
    ]
    if not rows:
        log.warning(f"[audit] {api_type} — no rows to write")
        return
    spark.createDataFrame(rows, schema=audit_schema) \
        .write.format("delta").mode("append").saveAsTable(LOGS_TABLE)
    log.info(f"[audit] {api_type} — {len(rows)} attempt(s) logged → {LOGS_TABLE}")


# ---------------------------------------------------------------------------
# Helper: append_bronze — appends one row per page to the bronze table
# ---------------------------------------------------------------------------
# Lookup: table suffix → centralized bronze table constant
BRONZE_TABLES = {
    "_charterparties":    BRONZE_CHARTERPARTIES,
    "_offhires":          BRONZE_OFFHIRES,
    "_reviews":           BRONZE_REVIEWS,
    "_ships":             BRONZE_SHIPS,
    "_ships_properties":  BRONZE_SHIPS_PROPERTIES,
}

def append_bronze(results, table_suffix: str) -> None:
    """
    Appends one row per page (SUCCESS) or a sentinel row (FAILED) to the bronze table.
    Accepts a single call_api result dict or a list of result dicts.
    For _ships_properties, includes ImoNr (the API input value) and uses properties_bronze_schema.
    """
    if isinstance(results, dict):
        results = [results]
    table_name   = BRONZE_TABLES[table_suffix]
    ingestion_ts = datetime.now(timezone.utc)
    is_properties = (table_suffix == "_ships_properties")
    schema = properties_bronze_schema if is_properties else bronze_schema
    rows = []
    for result in results:
        imo_nr = result["input_value"] if is_properties and result.get("input_value") else None
        if result["status"] == "SUCCESS" and result["pages"]:
            for page in result["pages"]:
                meta = json.dumps({
                    "Status":       "SUCCESS",
                    "ErrorMessage": None,
                    "HttpStatus":   page["http_status"],
                    "Timestamp":    page["request_timestamp"].isoformat(),
                })
                if is_properties:
                    # properties_bronze_schema order: ImoNr, Raw_data, Api_metadata, _is_normalized, _Batch_run_id, _ingestion_ts
                    rows.append((imo_nr, page["body"], meta, False, result["batch_run_id"], ingestion_ts))
                else:
                    # bronze_schema order: Raw_data, Api_metadata, _is_normalized, _Batch_run_id, _ingestion_ts
                    rows.append((page["body"], meta, False, result["batch_run_id"], ingestion_ts))
        else:
            last = result["attempts"][-1] if result["attempts"] else {}
            meta = json.dumps({
                "Status":       "FAILED",
                "ErrorMessage": last.get("error_message"),
                "HttpStatus":   last.get("response_code"),
                "Timestamp":    last["request_timestamp"].isoformat() if last.get("request_timestamp") else None,
            })
            if is_properties:
                rows.append((imo_nr, last.get("response_body"), meta, True, result["batch_run_id"], ingestion_ts))
            else:
                rows.append((last.get("response_body"), meta, True, result["batch_run_id"], ingestion_ts))
    if not rows:
        log.warning(f"[bronze] {table_suffix} — no rows to write")
        return
    spark.createDataFrame(rows, schema=schema) \
        .write.format("delta").mode("append").saveAsTable(table_name)
    log.info(f"[bronze] {table_suffix} — {len(rows)} row(s) appended → {table_name}")


# ---------------------------------------------------------------------------
# Helper: process_endpoint — call_api + log_audit + append_bronze
# ---------------------------------------------------------------------------
def process_endpoint(
    endpoint: str,
    suffix: str,
    headers: dict,
    input_value: str = None,
) -> dict:
    result = call_api("GET", f"{BASE_APIM}{endpoint}", headers, input_value=input_value)
    log_audit(result, suffix)
    append_bronze(result, suffix)
    if result["status"] == "FAILED":
        raise RuntimeError(
            f"API call FAILED for '{endpoint}'" + (f" (input: {input_value})" if input_value else "")
        )
    return result


# ---------------------------------------------------------------------------
# Helper: run_concurrent_ships_properties — threaded fetch of /ships/{imo}/properties
# ---------------------------------------------------------------------------
def run_concurrent_ships_properties(imo_list: list, max_workers: int = 5) -> None:
    """
    Fetches /ships/{imoNr}/properties for every IMO in imo_list using
    ThreadPoolExecutor (default 5 workers).
    Threads only call call_api — no I/O inside threads.
    Audit logging and bronze append are done once after all requests complete.
    """
    if not imo_list:
        log.warning("[ships/properties] imo_list is empty — skipping concurrent fetch")
        return

    log.info(f"[ships/properties] Starting concurrent fetch — {len(imo_list)} IMO(s), {max_workers} workers")

    results, errors, completed = [], 0, 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                call_api,
                "GET",
                f"{BASE_APIM}{SHIPS_PROPERTIES_ENDPOINT.format(imo)}",
                headers,
                str(imo),
            ): imo
            for imo in imo_list
        }
        for future in as_completed(futures):
            imo = futures[future]
            try:
                result = future.result()
                results.append(result)
                if result["status"] == "FAILED":
                    errors += 1
                    log.error(f"[ships/properties] IMO {imo} — API FAILED")
            except Exception as exc:
                errors += 1
                log.error(f"[ships/properties] IMO {imo} — EXCEPTION: {exc}")
            completed += 1
            if completed % 50 == 0:
                log.info(f"[ships/properties] Progress — {completed}/{len(imo_list)} requests completed, {errors} failed so far")

    log.info(f"[ships/properties] All requests done — {len(results)} collected, {errors} failed")

    # Batch I/O: one commit per sink after all threads complete
    log_audit(results, SHIPS_PROPERTIES_SUFFIX)
    append_bronze(results, SHIPS_PROPERTIES_SUFFIX)


# COMMAND ----------

# DBTITLE 1,Normalize Ships
# ---------------------------------------------------------------------------
# Helper: normalize_ships — bronze ANK_ships → silver (full refresh)
# ---------------------------------------------------------------------------
def normalize_ships() -> None:
    """
    Reads all pending bronze rows (_is_normalized=False) from ANK_ships,
    explodes the nested ships array, casts fields, and overwrites the silver
    table as a full refresh.  Marks processed bronze rows as normalized.
    """

    bronze_df = (
        spark.table(BRONZE_SHIPS)
        .filter(F.col("_is_normalized") == False)
        .orderBy(F.col("_ingestion_ts").desc())
        .limit(1)
    )

    parsed_df = (
        bronze_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _ships_page_schema))
        .withColumn("ship",    F.explode(F.col("_parsed.ships")))
        .select(
            F.col("ship.name")                .alias("name"),
            F.col("ship.imoNr")               .alias("imoNr"),
            F.col("ship.provisionalId")       .alias("provisionalId"),
            F.col("ship.profileCompleteness") .alias("profileCompleteness"),
            F.to_timestamp("ship.lastUpdated").alias("lastUpdated"),
            F.col("ship.isReporting")         .alias("isReporting"),
            F.col("ship.own")                 .alias("own"),
            F.col("ship.charter")             .alias("charter"),
            F.col("ship.shipType")            .alias("shipType"),
            F.col("_Batch_run_id"),
        )
        .withColumn("_ingestion_ts", F.current_timestamp())
    )

    row_count = parsed_df.count()
    (
        parsed_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_SHIPS)
    )
    log.info(f"[silver] ANK_ships — {row_count} row(s) written (full refresh) → {SILVER_SHIPS}")

# COMMAND ----------

# DBTITLE 1,Main
if __name__ == "__main__":
    log.info(f"Batch run started   | {NOTEBOOK_NAME} | batch_run_id={batch_run_id} | total_endpoints={len(ENDPOINTS)}")

    for i, (endpoint, suffix) in enumerate(ENDPOINTS.items(), 1):
        log.info(f"[{i}/{len(ENDPOINTS)}] GET {BASE_APIM}{endpoint}  →  {suffix}")
        process_endpoint(endpoint, suffix, headers)
    normalize_ships()

    # Resolve distinct IMOs from silver then fan-out concurrently
    imo_list = [
        row.ImoNr if row.ImoNr != 0 else row.ProvisionalId
        for row in spark.table(SILVER_SHIPS)
            .select("ImoNr", "ProvisionalId")
            .filter(F.col("ImoNr").isNotNull())
            .distinct()
            .collect()
    ]
    log.info(f"[ships/properties] {len(imo_list)} distinct IMO(s) resolved from silver")
    run_concurrent_ships_properties(imo_list)

    log.info(f"Batch run completed | {NOTEBOOK_NAME} | batch_run_id={batch_run_id}")


# COMMAND ----------

# DBTITLE 1,Normalize Charterparties Rates
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_charterparties → silver (charterparties_rates)
# ---------------------------------------------------------------------------

bronze_df = (
    spark.table(BRONZE_CHARTERPARTIES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

parsed_df = (
    bronze_df
    .withColumn("_parsed", F.from_json(F.col("Raw_data"), _cp_rates_schema))
    .select(
        F.explode(F.col("_parsed.contracts")).alias("contract"),
        F.col("_Batch_run_id"),
    )
    .select(
        F.col("contract.contractId")         .alias("contractId"),
        F.col("contract.rate_review_period") .alias("rate_review_period"),
        F.explode(F.col("contract.rates"))   .alias("rate_entry"),
        F.col("_Batch_run_id"),
    )
    .select(
        F.col("contractId"),
        F.col("rate_entry.rate").cast("double").alias("rate"),
        F.col("rate_entry.special")          .alias("special"),
        F.col("rate_entry.currency")         .alias("currency"),
        F.col("rate_entry.rate_uuid")        .alias("rate_uuid"),
        F.col("rate_entry.valid_from")       .alias("valid_from"),
        F.col("contractId").alias("lump_sums"),
        F.col("rate_review_period"),
        F.col("_Batch_run_id"),
        F.current_timestamp().alias("_ingestion_ts"),
    )
)

row_count = parsed_df.count()
(
    parsed_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_CHARTERPARTIES_RATES)
)
log.info(f"[silver] ANK_charterparties_rates — {row_count} row(s) written → {SILVER_CHARTERPARTIES_RATES}")

# COMMAND ----------

# DBTITLE 1,Normalize Charterparties Lump Sums
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_charterparties → silver (charterparties_lump_sums)
# ---------------------------------------------------------------------------

bronze_df = (
    spark.table(BRONZE_CHARTERPARTIES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_df.count() == 0:
    log.warning("[silver] ANK_charterparties_lump_sums — no pending bronze rows to normalize")
else:
    parsed_ls_df = (
        bronze_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _cp_lump_sums_schema))
        .select(
            F.explode(F.col("_parsed.contracts")).alias("contract"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("contract.contractId").alias("contractId"),
            F.explode(F.col("contract.rates")).alias("rate_entry"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("contractId"),
            F.explode(F.col("rate_entry.lump_sums")).alias("ls"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("contractId"),
            F.col("ls.type")          .alias("type"),
            F.col("ls.ls_unit")       .alias("ls_unit"),
            F.col("ls.ls_amount").cast("double").alias("ls_amount"),
            F.col("ls.ls_currency")   .alias("ls_currency"),
            F.col("ls.rate_number")   .alias("rate_number"),
            F.col("ls.lump_sum_uuid") .alias("lump_sum_uuid"),
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )

    ls_row_count = parsed_ls_df.count()
    (
        parsed_ls_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_CHARTERPARTIES_LUMP_SUMS)
    )
    log.info(f"[silver] ANK_charterparties_lump_sums — {ls_row_count} row(s) written → {SILVER_CHARTERPARTIES_LUMP_SUMS}")

# COMMAND ----------

# DBTITLE 1,Normalize Charterparties At Cost
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_charterparties → silver (charterparties_at_cost)
# ---------------------------------------------------------------------------

bronze_df = (
    spark.table(BRONZE_CHARTERPARTIES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_df.count() == 0:
    log.warning("[silver] ANK_charterparties_at_cost — no pending bronze rows to normalize")
else:
    parsed_atc_df = (
        bronze_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _cp_at_cost_schema))
        .select(
            F.explode(F.col("_parsed.contracts")).alias("contract"),
            F.col("_Batch_run_id"),
        )
        .withColumn("atCost_arr", F.from_json(
            F.col("contract.atCost"),
            "ARRAY<STRUCT<at_cost_type: STRING, at_cost_unit: STRING, at_cost_amount: STRING, at_cost_comment: STRING>>"
        ))
        .select(
            F.col("contract.contractId").alias("contractId"),
            F.explode(F.col("atCost_arr")).alias("atc"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("contractId"),
            F.col("atc.at_cost_type")   .alias("at_cost_type"),
            F.col("atc.at_cost_unit")   .alias("at_cost_unit"),
            F.when(F.trim(F.col("atc.at_cost_amount")) == "", None).otherwise(F.col("atc.at_cost_amount")).cast("double").alias("at_cost_amount"),
            F.col("atc.at_cost_comment").alias("at_cost_comment"),
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )

    atc_row_count = parsed_atc_df.count()
    (
        parsed_atc_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_CHARTERPARTIES_ATCOST)
    )
    log.info(f"[silver] ANK_charterparties_at_cost — {atc_row_count} row(s) written → {SILVER_CHARTERPARTIES_ATCOST}")

# COMMAND ----------

# DBTITLE 1,Normalize Charterparties Extension
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_charterparties → silver (charterparties_extension)
# ---------------------------------------------------------------------------

# Helper: convert string boolean ('true','1','false','0') → int (1/0/None)
def _bool_to_int(col):
    return (
        F.when(col.cast("string").isin("true", "1"), F.lit(1))
         .when(col.cast("string").isin("false", "0"), F.lit(0))
         .otherwise(F.lit(None).cast("int"))
    )

bronze_df = (
    spark.table(BRONZE_CHARTERPARTIES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_df.count() == 0:
    log.warning("[silver] ANK_charterparties_extension — no pending bronze rows to normalize")
else:
    parsed_ext_df = (
        bronze_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _cp_charterparties_schema))
        .select(
            F.explode(F.col("_parsed.contracts")).alias("c"),
            F.col("_Batch_run_id"),
        )
        .filter(F.col("c.contractType") == "extension")
        .select(
            # --- Identifiers (cast to long) ---
            F.col("c.contractId").cast("long").alias("contractId"),
            F.col("c.contractType").alias("contractType"),
            F.col("c.contractPeriod").alias("contractPeriod"),
            F.col("c.shipImoNr").cast("string").alias("shipImoNr"),
            F.col("c.shipName").alias("shipName"),
            # --- Charter type (mapped) ---
            F.when(F.col("c.charterType") == "time_charter", "Time Charter")
             .when(F.col("c.charterType") == "bareboat_charter", "Bare Boat")
             .otherwise(F.col("c.charterType")).alias("charterType"),
            F.col("c.chartererId").cast("long").alias("chartererId"),
            F.col("c.chartererName").alias("chartererName"),
            F.col("c.currentOwnerId").cast("long").alias("currentOwnerId"),
            F.col("c.ownerName").alias("ownerName"),
            F.col("c.brokerId").cast("long").alias("brokerId"),
            F.col("c.brokerName").alias("brokerName"),
            _bool_to_int(F.col("c.brokerIsCompetitive")).alias("brokerIsCompetitive"),
            # --- Dates ---
            F.col("c.addendumDate").alias("addendumDate"),
            F.col("c.addendumNumber").alias("addendumNumber"),
            F.col("c.bodApprovalDate").alias("bodApprovalDate"),
            F.col("c.bod_approval_date_charterer").alias("bod_approval_date_charterer"),
            F.to_timestamp("c.canDate").alias("canDate"),
            F.to_timestamp("c.cancellingDate").alias("cancellingDate"),
            _bool_to_int(F.col("c.cancellingDateIsUtc")).alias("cancellingDateIsUtc"),
            F.col("c.charterPartyId").alias("charterPartyId"),
            F.to_timestamp("c.cpDate").alias("cpDate"),
            F.to_timestamp("c.created").alias("created"),
            F.to_timestamp("c.declarationDate").alias("declarationDate"),
            F.col("c.declarationType").alias("declarationType"),
            F.to_timestamp("c.deliveryDate").alias("deliveryDate"),
            F.when(F.trim(F.col("c.deliveryPortArea")) == "", None)
             .otherwise(F.col("c.deliveryPortArea")).alias("deliveryPortArea"),
            F.col("c.deliveryPortPlace").alias("deliveryPortPlace"),
            F.col("c.deliveryStatus").alias("deliveryStatus"),
            F.col("c.dryDockClauseComment").alias("dryDockClauseComment"),
            # --- ETS fields (booleans → int) ---
            _bool_to_int(F.col("c.etsClause")).alias("etsClause"),
            F.col("c.etsComment").alias("etsComment"),
            F.col("c.etsNotifyChartererWithin").alias("etsNotifyChartererWithin"),
            F.col("c.etsOptionToSettleEuaInCash").alias("etsOptionToSettleEuaInCash"),
            F.col("c.etsOptionToSettleEuaInCashPeriod").alias("etsOptionToSettleEuaInCashPeriod"),
            F.col("c.etsPaymentFulfilment").alias("etsPaymentFulfilment"),
            F.col("c.etsReportingPeriod").alias("etsReportingPeriod"),
            F.col("c.etsReportingPeriodSplit").alias("etsReportingPeriodSplit"),
            F.col("c.etsReportingRedelivery").alias("etsReportingRedelivery"),
            F.col("c.etsReportingRedeliveryDays").alias("etsReportingRedeliveryDays"),
            F.col("c.etsReportingRedeliveryMonths").alias("etsReportingRedeliveryMonths"),
            F.col("c.etsSettlementRedelivery").alias("etsSettlementRedelivery"),
            F.col("c.etsSettlementRedeliveryDays").alias("etsSettlementRedeliveryDays"),
            F.col("c.etsSettlementRedeliveryMonths").alias("etsSettlementRedeliveryMonths"),
            F.col("c.etsTransferOfEuaToOwner").alias("etsTransferOfEuaToOwner"),
            F.col("c.etsTransferOfEuaToOwnerDate").alias("etsTransferOfEuaToOwnerDate"),
            F.col("c.etsTransferOfEuaToOwnerNumeric").alias("etsTransferOfEuaToOwnerNumeric"),
            # --- Extension / redelivery ---
            F.col("c.extensionDate").alias("extensionDate"),
            F.col("c.fixtureDate").alias("fixtureDate"),
            F.to_timestamp("c.fromDate").alias("fromDate"),
            _bool_to_int(F.col("c.fromDateIsPlanned")).alias("fromDateIsPlanned"),
            F.col("c.fromDateTimezone").alias("fromDateTimezone"),
            F.to_timestamp("c.toDate").alias("toDate"),
            _bool_to_int(F.col("c.toDateIsPlanned")).alias("toDateIsPlanned"),
            F.col("c.toDateTimezone").alias("toDateTimezone"),
            _bool_to_int(F.col("c.hasPersistedLumpsums")).alias("hasPersistedLumpsums"),
            _bool_to_int(F.col("c.hireInvoiceRequired")).alias("hireInvoiceRequired"),
            F.col("c.ifrsRelevant").alias("ifrsRelevant"),
            _bool_to_int(F.col("c.isDryDockClause")).alias("isDryDockClause"),
            F.col("c.lastModified").alias("lastModified"),
            F.to_timestamp("c.layDate").alias("layDate"),
            _bool_to_int(F.col("c.layDateIsUtc")).alias("layDateIsUtc"),
            F.to_timestamp("c.laydaysDate").alias("laydaysDate"),
            F.col("c.mainTermsDate").alias("mainTermsDate"),
            F.col("c.optionToAddOffhireDays").alias("optionToAddOffhireDays"),
            F.col("c.optionToAddOffhireDaysComment").alias("optionToAddOffhireDaysComment"),
            F.col("c.option_type").alias("option_type"),
            F.col("c.period").alias("period"),
            F.col("c.fixerEmail").alias("fixerEmail"),
            F.col("c.postfixerEmail").alias("postfixerEmail"),
            # --- Rate fields (decimals + mapped rate_type) ---
            F.get(F.col("c.rateBillingPeriods"), 0).getField("rbp").alias("rateBillingPeriod"),
            F.when(F.trim(F.get(F.col("c.rateBillingPeriods"), 0).getField("comment")) == "", None)
             .otherwise(F.get(F.col("c.rateBillingPeriods"), 0).getField("comment")).alias("rateBillingPeriodComment"),
            F.col("c.rate_ceiling").alias("rate_ceiling"),
            F.col("c.rate_comment").alias("rate_comment"),
            F.col("c.rate_discount_premium").cast("decimal(38,6)").alias("rate_discount_premium"),
            F.col("c.rate_floor").alias("rate_floor"),
            F.col("c.rate_review_period").alias("rate_review_period"),
            F.when(F.col("c.rate_type") == "fixed", "Fixed")
             .when(F.col("c.rate_type") == "contex", "Contex")
             .when(F.col("c.rate_type") == "market_evaluation", "Market")
             .otherwise(F.col("c.rate_type")).alias("rate_type"),
            # --- Redelivery ---
            F.to_timestamp("c.redeliveryDate").alias("redeliveryDate"),
            F.to_timestamp("c.redeliveryEndDate").alias("redeliveryEndDate"),
            F.col("c.redeliveryPlan").alias("redeliveryPlan"),
            F.col("c.redeliveryPortPlace").alias("redeliveryPortPlace"),
            F.col("c.redeliveryRange").alias("redeliveryRange"),
            F.to_timestamp("c.redeliveryStartDate").alias("redeliveryStartDate"),
            F.col("c.redeliveryStatus").alias("redeliveryStatus"),
            # --- Clauses (booleans → int) ---
            _bool_to_int(F.col("c.bunker_clause")).alias("bunker_clause"),
            F.col("c.bunker_clause_comment").alias("bunker_clause_comment"),
            _bool_to_int(F.col("c.war_insurance_clause")).alias("war_insurance_clause"),
            F.col("c.war_insurance_arranged_by").alias("war_insurance_arranged_by"),
            F.col("c.war_insurance_paid_by").alias("war_insurance_paid_by"),
            _bool_to_int(F.col("c.ciiClause")).alias("ciiClause"),
            F.col("c.ciiComment").alias("ciiComment"),
            F.col("c.ciiRatingAtDelivery").alias("ciiRatingAtDelivery"),
            F.col("c.ciiRatingAtRedelivery").alias("ciiRatingAtRedelivery"),
            F.col("c.comments").alias("comments"),
            F.col("c.commentsForNextExtension").alias("commentsForNextExtension"),
            # --- Address commission (decimal) ---
            F.get_json_object(F.col("c.addressCommission"), "$.amount").cast("decimal(38,6)").alias("addressCommissionAmount"),
            # --- External fields ---
            F.col("c.external_fields.account_id").alias("account_id"),
            F.col("c.external_fields.crmd_commission").alias("crmd_commission"),
            F.to_timestamp("c.external_fields.crmd_earliesttcenddateupdate").alias("earliesttcenddateupdate"),
            F.col("c.external_fields.crmd_finalizedrate").cast("decimal(38,6)").alias("finalizedrate"),
            F.col("c.external_fields.crmd_hull").alias("crmd_hull"),
            F.col("c.external_fields.crmd_name").alias("crmd_name"),
            F.col("c.external_fields.fixture_id").alias("fixture_id"),
            F.col("c.external_fields.rate_id").alias("rate_id"),
            F.col("c.external_fields.statuscode").cast("long").alias("statuscode"),
            F.col("c.external_fields.statuscodeName").alias("statuscodeName"),
            # --- Metadata ---
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )

    ext_row_count = parsed_ext_df.count()
    (
        parsed_ext_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_CHARTERPARTIES_EXTENSION)
    )
    log.info(f"[silver] ANK_charterparties_extension — {ext_row_count} row(s) written → {SILVER_CHARTERPARTIES_EXTENSION}")

# COMMAND ----------

# DBTITLE 1,Normalize Charterparties (Combined)
# --- 1. Parse charterparty contracts from bronze (same schema as extension) ---
bronze_df = (
    spark.table(BRONZE_CHARTERPARTIES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_df.count() == 0:
    log.warning("[silver] ANK_charterparties — no pending bronze rows to normalize")
else:
    ch_df = (
        bronze_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _cp_charterparties_schema))
        .select(
            F.explode(F.col("_parsed.contracts")).alias("c"),
            F.col("_Batch_run_id"),
        )
        .filter(F.col("c.contractType") == "charterparty")
        .select(
            # --- Identifiers ---
            F.col("c.contractId").cast("long").alias("contractId"),
            F.col("c.contractType").alias("contractType"),
            F.col("c.contractPeriod").alias("contractPeriod"),
            F.col("c.shipImoNr").alias("Vessel_IMO"),
            F.col("c.shipName").alias("shipName"),
            # --- Charter type (mapped) ---
            F.when(F.col("c.charterType") == "time_charter", "Time Charter")
             .when(F.col("c.charterType") == "bareboat_charter", "Bare Boat")
             .otherwise(F.col("c.charterType")).alias("charterType"),
            F.col("c.chartererId").cast("long").alias("chartererId"),
            F.col("c.chartererName").alias("chartererName"),
            F.col("c.currentOwnerId").cast("long").alias("currentOwnerId"),
            F.col("c.ownerName").alias("ownerName"),
            F.col("c.brokerId").cast("long").alias("brokerId"),
            F.col("c.brokerName").alias("brokerName"),
            _bool_to_int(F.col("c.brokerIsCompetitive")).alias("brokerIsCompetitive"),
            # --- Dates ---
            F.col("c.addendumDate").alias("addendumDate"),
            F.col("c.addendumNumber").alias("addendumNumber"),
            F.col("c.bodApprovalDate").alias("bodApprovalDate"),
            F.col("c.bod_approval_date_charterer").alias("bod_approval_date_charterer"),
            F.to_timestamp("c.canDate").alias("canDate"),
            F.to_timestamp("c.cancellingDate").alias("cancellingDate"),
            _bool_to_int(F.col("c.cancellingDateIsUtc")).alias("cancellingDateIsUtc"),
            F.col("c.charterPartyId").alias("charterPartyId"),
            F.to_timestamp("c.cpDate").alias("cpDate"),
            F.to_timestamp("c.created").alias("created"),
            F.to_timestamp("c.declarationDate").alias("declarationDate"),
            F.col("c.declarationType").alias("declarationType"),
            F.to_timestamp("c.deliveryDate").alias("deliveryDate"),
            F.when(F.trim(F.col("c.deliveryPortArea")) == "", None)
             .otherwise(F.col("c.deliveryPortArea")).alias("deliveryPortArea"),
            F.col("c.deliveryPortPlace").alias("deliveryPortPlace"),
            F.col("c.deliveryStatus").alias("deliveryStatus"),
            F.col("c.dryDockClauseComment").alias("dryDockClauseComment"),
            # --- ETS fields (booleans → int) ---
            _bool_to_int(F.col("c.etsClause")).alias("etsClause"),
            F.col("c.etsComment").alias("etsComment"),
            F.col("c.etsNotifyChartererWithin").alias("etsNotifyChartererWithin"),
            F.col("c.etsOptionToSettleEuaInCash").alias("etsOptionToSettleEuaInCash"),
            F.col("c.etsOptionToSettleEuaInCashPeriod").alias("etsOptionToSettleEuaInCashPeriod"),
            F.col("c.etsPaymentFulfilment").alias("etsPaymentFulfilment"),
            F.col("c.etsReportingPeriod").alias("etsReportingPeriod"),
            F.col("c.etsReportingPeriodSplit").alias("etsReportingPeriodSplit"),
            F.col("c.etsReportingRedelivery").alias("etsReportingRedelivery"),
            F.col("c.etsReportingRedeliveryDays").alias("etsReportingRedeliveryDays"),
            F.col("c.etsReportingRedeliveryMonths").alias("etsReportingRedeliveryMonths"),
            F.col("c.etsSettlementRedelivery").alias("etsSettlementRedelivery"),
            F.col("c.etsSettlementRedeliveryDays").alias("etsSettlementRedeliveryDays"),
            F.col("c.etsSettlementRedeliveryMonths").alias("etsSettlementRedeliveryMonths"),
            F.col("c.etsTransferOfEuaToOwner").alias("etsTransferOfEuaToOwner"),
            F.col("c.etsTransferOfEuaToOwnerDate").alias("etsTransferOfEuaToOwnerDate"),
            F.col("c.etsTransferOfEuaToOwnerNumeric").alias("etsTransferOfEuaToOwnerNumeric"),
            # --- Extension / redelivery ---
            F.col("c.extensionDate").alias("extensionDate"),
            F.col("c.fixtureDate").alias("fixtureDate"),
            F.to_timestamp("c.fromDate").alias("fromDate"),
            _bool_to_int(F.col("c.fromDateIsPlanned")).alias("fromDateIsPlanned"),
            F.col("c.fromDateTimezone").alias("fromDateTimezone"),
            F.to_timestamp("c.toDate").alias("toDate"),
            _bool_to_int(F.col("c.toDateIsPlanned")).alias("toDateIsPlanned"),
            F.col("c.toDateTimezone").alias("toDateTimezone"),
            _bool_to_int(F.col("c.hasPersistedLumpsums")).alias("hasPersistedLumpsums"),
            _bool_to_int(F.col("c.hireInvoiceRequired")).alias("hireInvoiceRequired"),
            F.col("c.ifrsRelevant").alias("ifrsRelevant"),
            _bool_to_int(F.col("c.isDryDockClause")).alias("isDryDockClause"),
            F.col("c.lastModified").alias("lastModified"),
            F.to_timestamp("c.layDate").alias("layDate"),
            _bool_to_int(F.col("c.layDateIsUtc")).alias("layDateIsUtc"),
            F.to_timestamp("c.laydaysDate").alias("laydaysDate"),
            F.col("c.mainTermsDate").alias("mainTermsDate"),
            F.col("c.optionToAddOffhireDays").alias("optionToAddOffhireDays"),
            F.col("c.optionToAddOffhireDaysComment").alias("optionToAddOffhireDaysComment"),
            F.col("c.option_type").alias("option_type"),
            F.col("c.period").alias("period"),
            F.col("c.fixerEmail").alias("fixerEmail"),
            F.col("c.postfixerEmail").alias("postfixerEmail"),
            # --- Rate fields ---
            F.get(F.col("c.rateBillingPeriods"), 0).getField("rbp").alias("rateBillingPeriod"),
            F.when(F.trim(F.get(F.col("c.rateBillingPeriods"), 0).getField("comment")) == "", None)
             .otherwise(F.get(F.col("c.rateBillingPeriods"), 0).getField("comment")).alias("rateBillingPeriodComment"),
            F.col("c.rate_ceiling").alias("rate_ceiling"),
            F.col("c.rate_comment").alias("rate_comment"),
            F.col("c.rate_discount_premium").cast("decimal(38,6)").alias("rate_discount_premium"),
            F.col("c.rate_floor").alias("rate_floor"),
            F.col("c.rate_review_period").alias("rate_review_period"),
            F.when(F.col("c.rate_type") == "fixed", "Fixed")
             .when(F.col("c.rate_type") == "contex", "Contex")
             .when(F.col("c.rate_type") == "market_evaluation", "Market")
             .otherwise(F.col("c.rate_type")).alias("rate_type"),
            # --- Redelivery ---
            F.to_timestamp("c.redeliveryDate").alias("redeliveryDate"),
            F.to_timestamp("c.redeliveryEndDate").alias("redeliveryEndDate"),
            F.col("c.redeliveryPlan").alias("redeliveryPlan"),
            F.col("c.redeliveryPortPlace").alias("redeliveryPortPlace"),
            F.col("c.redeliveryRange").alias("redeliveryRange"),
            F.to_timestamp("c.redeliveryStartDate").alias("redeliveryStartDate"),
            F.col("c.redeliveryStatus").alias("redeliveryStatus"),
            # --- Clauses (booleans → int) ---
            _bool_to_int(F.col("c.bunker_clause")).alias("bunker_clause"),
            F.col("c.bunker_clause_comment").alias("bunker_clause_comment"),
            _bool_to_int(F.col("c.war_insurance_clause")).alias("war_insurance_clause"),
            F.col("c.war_insurance_arranged_by").alias("war_insurance_arranged_by"),
            F.col("c.war_insurance_paid_by").alias("war_insurance_paid_by"),
            _bool_to_int(F.col("c.ciiClause")).alias("ciiClause"),
            F.col("c.ciiComment").alias("ciiComment"),
            F.col("c.ciiRatingAtDelivery").alias("ciiRatingAtDelivery"),
            F.col("c.ciiRatingAtRedelivery").alias("ciiRatingAtRedelivery"),
            F.col("c.comments").alias("comments"),
            F.col("c.commentsForNextExtension").alias("commentsForNextExtension"),
            # --- Address commission ---
            F.get_json_object(F.col("c.addressCommission"), "$.amount").cast("decimal(38,6)").alias("addressCommissionAmount"),
            # --- External fields ---
            F.col("c.external_fields.account_id").alias("account_id"),
            F.col("c.external_fields.crmd_commission").alias("crmd_commission"),
            F.to_timestamp("c.external_fields.crmd_earliesttcenddateupdate").alias("earliesttcenddateupdate"),
            F.col("c.external_fields.crmd_finalizedrate").cast("decimal(38,6)").alias("finalizedrate"),
            F.col("c.external_fields.crmd_hull").alias("crmd_hull"),
            F.col("c.external_fields.crmd_name").alias("crmd_name"),
            F.col("c.external_fields.fixture_id").alias("fixture_id"),
            F.col("c.external_fields.rate_id").alias("rate_id"),
            F.col("c.external_fields.statuscode").cast("long").alias("statuscode"),
            F.col("c.external_fields.statuscodeName").alias("statuscodeName"),
            # --- Metadata ---
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )
    _vessel_df = spark.table(VESSEL_TABLE).select(
        F.col("Hull_no"),
        F.col("imo").alias("_v_imo"),
    )
    ch_df = (
        ch_df
        .join(_vessel_df, on=(F.col("Vessel_IMO").cast("string") == F.col("Hull_no")), how="left")
        .withColumn("Vessel_IMO", F.coalesce(F.col("_v_imo").cast("long"), F.col("Vessel_IMO").cast("long")))
        .drop("Hull_no", "_v_imo")
    )

    # --- 2. Extension contracts from silver, joined with document lookup ---
    ext_df = spark.table(SILVER_CHARTERPARTIES_EXTENSION)
    doc_lookup_df = spark.table(DOCUMENT_LOOKUP)

    ex_df = (
        ext_df.alias("a")
        .join(
            doc_lookup_df.alias("b"),
            on=(
                (F.col("a.crmd_name") == F.col("b.fixture_ID"))
                & (
                    F.col("a.extensionDate").cast("date")
                    == F.when(
                        F.col("b.extension_date").cast("date") < F.lit("1990-01-01").cast("date"),
                        F.add_months(F.col("b.extension_date").cast("date"), 12 * 100),
                    ).otherwise(F.col("b.extension_date").cast("date"))
                )
            ),
            how="left",
        )
        .select(
            F.col("a.*"),
            F.col("b.crmd_name").alias("fixture_name"),
        )
        # Replace crmd_name with fixture_name from document lookup
        .withColumn("crmd_name", F.col("fixture_name"))
        .withColumnRenamed("shipImoNr", "Vessel_IMO")
        .withColumn("Vessel_IMO", F.col("Vessel_IMO"))
        .drop("fixture_name")
    )

    # NULL out external fields for extension rows (post_hook equivalent)
    ext_null_cols = [
        "crmd_hull", "crmd_commission", "rate_id", "account_id",
        "fixture_id", "statuscode", "statuscodeName", "finalizedrate",
        "earliesttcenddateupdate",
    ]
    for col_name in ext_null_cols:
        ex_df = ex_df.withColumn(col_name, F.lit(None).cast(ex_df.schema[col_name].dataType))

    ex_df = (
        ex_df
        .join(_vessel_df, on=(F.col("Vessel_IMO").cast("string") == F.col("Hull_no")), how="left")
        .withColumn("Vessel_IMO", F.coalesce(F.col("_v_imo").cast("long"), F.col("Vessel_IMO").cast("long")))
        .drop("Hull_no", "_v_imo")
    )

    # --- 3. Union charterparty + extension ---
    final_df = ch_df.unionByName(ex_df, allowMissingColumns=True)

    # --- 4. Resolve active rate per contract ---
    rates_df = (
        spark.table(SILVER_CHARTERPARTIES_RATES)
        .select(
            F.col("contractId"),
            F.col("rate").cast("double").alias("rate"),
            F.col("valid_from"),
            F.col("currency"),
        )
        .join(final_df.select("contractId", "deliveryDate", "extensionDate", "period"), on="contractId", how="inner")
        .withColumn(
            "valid_from_resolved",
            F.when(F.col("valid_from").isNull(), F.coalesce("deliveryDate", "extensionDate"))
             .otherwise(F.to_timestamp("valid_from")),
        )
    )

    # Window: pick latest valid_from for past/ongoing, earliest for upcoming
    w_desc = W.partitionBy("contractId", "period").orderBy(F.col("valid_from_resolved").desc())
    w_asc  = W.partitionBy("contractId", "period").orderBy(F.col("valid_from_resolved").asc())

    filtered_rates_df = (
        rates_df
        .filter(
            (F.col("period") == "past")
            | ((F.col("period") == "ongoing") & (F.col("valid_from_resolved") <= F.current_timestamp()))
            | (F.col("period") == "upcoming")
        )
        .withColumn("rn", F.row_number().over(w_desc))
        .withColumn("rn1", F.row_number().over(w_asc))
    )

    finalized_rate_df = (
        filtered_rates_df
        .filter(
            ((F.col("period").isin("past", "ongoing")) & (F.col("rn") == 1))
            | ((F.col("period") == "upcoming") & (F.col("rn1") == 1))
        )
        .select(
            F.col("contractId"),
            F.col("rate").alias("final_rate"),
            F.col("valid_from_resolved").cast("timestamp").alias("rate_start"),
            F.col("currency").alias("rate_currency"),
        )
    )

    # --- 5. Final join: add rate + expectedRedeliveryDate ---
    result_df = (
        final_df
        .join(finalized_rate_df, on="contractId", how="left")
        .withColumn(
            "expectedRedeliveryDate",
            F.when(F.col("toDateIsPlanned") == 1, F.col("redeliveryDate")),
        )
    )

    row_count = result_df.count()
    (
        result_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_CHARTERPARTIES)
    )
    log.info(f"[silver] ANK_charterparties — {row_count} row(s) written → {SILVER_CHARTERPARTIES}")

# COMMAND ----------

# DBTITLE 1,Normalize Charterparties Option
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_charterparties → silver (charterparties_option)
# Parses option contracts and resolves the active rate per contract.
# ---------------------------------------------------------------------------

bronze_opt_df = (
    spark.table(BRONZE_CHARTERPARTIES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_opt_df.count() == 0:
    log.warning("[silver] ANK_charterparties_option — no pending bronze rows to normalize")
else:
    opt_df = (
        bronze_opt_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _cp_charterparties_schema))
        .select(
            F.explode(F.col("_parsed.contracts")).alias("c"),
            F.col("_Batch_run_id"),
        )
        .filter(F.col("c.contractType") == "option")
        .select(
            # --- Identifiers ---
            F.col("c.contractId").cast("long").alias("contractId"),
            F.col("c.contractType").alias("contractType"),
            F.col("c.contractPeriod").alias("contractPeriod"),
            F.col("c.shipImoNr").alias("shipImoNr"),
            F.col("c.shipName").alias("shipName"),
            # --- Charter type (mapped) ---
            F.when(F.col("c.charterType") == "time_charter", "Time Charter")
             .when(F.col("c.charterType") == "bareboat_charter", "Bare Boat")
             .otherwise(F.col("c.charterType")).alias("charterType"),
            F.col("c.chartererId").cast("long").alias("chartererId"),
            F.col("c.chartererName").alias("chartererName"),
            F.col("c.currentOwnerId").cast("long").alias("currentOwnerId"),
            F.col("c.ownerName").alias("ownerName"),
            F.col("c.brokerId").cast("long").alias("brokerId"),
            F.col("c.brokerName").alias("brokerName"),
            _bool_to_int(F.col("c.brokerIsCompetitive")).alias("brokerIsCompetitive"),
            # --- Dates ---
            F.col("c.addendumDate").alias("addendumDate"),
            F.col("c.addendumNumber").alias("addendumNumber"),
            F.col("c.bodApprovalDate").alias("bodApprovalDate"),
            F.col("c.bod_approval_date_charterer").alias("bod_approval_date_charterer"),
            F.to_timestamp("c.canDate").alias("canDate"),
            F.to_timestamp("c.cancellingDate").alias("cancellingDate"),
            _bool_to_int(F.col("c.cancellingDateIsUtc")).alias("cancellingDateIsUtc"),
            F.col("c.charterPartyId").alias("charterPartyId"),
            F.to_timestamp("c.cpDate").alias("cpDate"),
            F.to_timestamp("c.created").alias("created"),
            F.to_timestamp("c.declarationDate").alias("declarationDate"),
            F.col("c.declarationType").alias("declarationType"),
            F.to_timestamp("c.deliveryDate").alias("deliveryDate"),
            F.when(F.trim(F.col("c.deliveryPortArea")) == "", None)
             .otherwise(F.col("c.deliveryPortArea")).alias("deliveryPortArea"),
            F.col("c.deliveryPortPlace").alias("deliveryPortPlace"),
            F.col("c.deliveryStatus").alias("deliveryStatus"),
            F.col("c.dryDockClauseComment").alias("dryDockClauseComment"),
            # --- ETS fields (booleans → int) ---
            _bool_to_int(F.col("c.etsClause")).alias("etsClause"),
            F.col("c.etsComment").alias("etsComment"),
            F.col("c.etsNotifyChartererWithin").alias("etsNotifyChartererWithin"),
            F.col("c.etsOptionToSettleEuaInCash").alias("etsOptionToSettleEuaInCash"),
            F.col("c.etsOptionToSettleEuaInCashPeriod").alias("etsOptionToSettleEuaInCashPeriod"),
            F.col("c.etsPaymentFulfilment").alias("etsPaymentFulfilment"),
            F.col("c.etsReportingPeriod").alias("etsReportingPeriod"),
            F.col("c.etsReportingPeriodSplit").alias("etsReportingPeriodSplit"),
            F.col("c.etsReportingRedelivery").alias("etsReportingRedelivery"),
            F.col("c.etsReportingRedeliveryDays").alias("etsReportingRedeliveryDays"),
            F.col("c.etsReportingRedeliveryMonths").alias("etsReportingRedeliveryMonths"),
            F.col("c.etsSettlementRedelivery").alias("etsSettlementRedelivery"),
            F.col("c.etsSettlementRedeliveryDays").alias("etsSettlementRedeliveryDays"),
            F.col("c.etsSettlementRedeliveryMonths").alias("etsSettlementRedeliveryMonths"),
            F.col("c.etsTransferOfEuaToOwner").alias("etsTransferOfEuaToOwner"),
            F.col("c.etsTransferOfEuaToOwnerDate").alias("etsTransferOfEuaToOwnerDate"),
            F.col("c.etsTransferOfEuaToOwnerNumeric").alias("etsTransferOfEuaToOwnerNumeric"),
            # --- Extension / redelivery ---
            F.col("c.extensionDate").alias("extensionDate"),
            F.col("c.fixtureDate").alias("fixtureDate"),
            F.to_timestamp("c.fromDate").alias("fromDate"),
            _bool_to_int(F.col("c.fromDateIsPlanned")).alias("fromDateIsPlanned"),
            F.col("c.fromDateTimezone").alias("fromDateTimezone"),
            F.to_timestamp("c.toDate").alias("toDate"),
            _bool_to_int(F.col("c.toDateIsPlanned")).alias("toDateIsPlanned"),
            F.col("c.toDateTimezone").alias("toDateTimezone"),
            _bool_to_int(F.col("c.hasPersistedLumpsums")).alias("hasPersistedLumpsums"),
            _bool_to_int(F.col("c.hireInvoiceRequired")).alias("hireInvoiceRequired"),
            F.col("c.ifrsRelevant").alias("ifrsRelevant"),
            _bool_to_int(F.col("c.isDryDockClause")).alias("isDryDockClause"),
            F.col("c.lastModified").alias("lastModified"),
            F.to_timestamp("c.layDate").alias("layDate"),
            _bool_to_int(F.col("c.layDateIsUtc")).alias("layDateIsUtc"),
            F.to_timestamp("c.laydaysDate").alias("laydaysDate"),
            F.col("c.mainTermsDate").alias("mainTermsDate"),
            F.col("c.optionToAddOffhireDays").alias("optionToAddOffhireDays"),
            F.col("c.optionToAddOffhireDaysComment").alias("optionToAddOffhireDaysComment"),
            F.col("c.option_type").alias("option_type"),
            F.col("c.period").alias("period"),
            F.col("c.fixerEmail").alias("fixerEmail"),
            F.col("c.postfixerEmail").alias("postfixerEmail"),
            # --- Rate fields ---
            F.get(F.col("c.rateBillingPeriods"), 0).getField("rbp").alias("rateBillingPeriod"),
            F.when(F.trim(F.get(F.col("c.rateBillingPeriods"), 0).getField("comment")) == "", None)
             .otherwise(F.get(F.col("c.rateBillingPeriods"), 0).getField("comment")).alias("rateBillingPeriodComment"),
            F.col("c.rate_ceiling").alias("rate_ceiling"),
            F.col("c.rate_comment").alias("rate_comment"),
            F.col("c.rate_discount_premium").cast("decimal(38,6)").alias("rate_discount_premium"),
            F.col("c.rate_floor").alias("rate_floor"),
            F.col("c.rate_review_period").alias("rate_review_period"),
            F.when(F.col("c.rate_type") == "fixed", "Fixed")
             .when(F.col("c.rate_type") == "contex", "Contex")
             .when(F.col("c.rate_type") == "market_evaluation", "Market")
             .otherwise(F.col("c.rate_type")).alias("rate_type"),
            # --- Redelivery ---
            F.to_timestamp("c.redeliveryDate").alias("redeliveryDate"),
            F.to_timestamp("c.redeliveryEndDate").alias("redeliveryEndDate"),
            F.col("c.redeliveryPlan").alias("redeliveryPlan"),
            F.col("c.redeliveryPortPlace").alias("redeliveryPortPlace"),
            F.col("c.redeliveryRange").alias("redeliveryRange"),
            F.to_timestamp("c.redeliveryStartDate").alias("redeliveryStartDate"),
            F.col("c.redeliveryStatus").alias("redeliveryStatus"),
            # --- Clauses (booleans → int) ---
            _bool_to_int(F.col("c.bunker_clause")).alias("bunker_clause"),
            F.col("c.bunker_clause_comment").alias("bunker_clause_comment"),
            _bool_to_int(F.col("c.war_insurance_clause")).alias("war_insurance_clause"),
            F.col("c.war_insurance_arranged_by").alias("war_insurance_arranged_by"),
            F.col("c.war_insurance_paid_by").alias("war_insurance_paid_by"),
            _bool_to_int(F.col("c.ciiClause")).alias("ciiClause"),
            F.col("c.ciiComment").alias("ciiComment"),
            F.col("c.ciiRatingAtDelivery").alias("ciiRatingAtDelivery"),
            F.col("c.ciiRatingAtRedelivery").alias("ciiRatingAtRedelivery"),
            F.col("c.comments").alias("comments"),
            F.col("c.commentsForNextExtension").alias("commentsForNextExtension"),
            # --- Address commission ---
            F.get_json_object(F.col("c.addressCommission"), "$.amount").cast("decimal(38,6)").alias("addressCommissionAmount"),
            # --- External fields ---
            F.col("c.external_fields.account_id").alias("account_id"),
            F.col("c.external_fields.crmd_commission").alias("crmd_commission"),
            F.to_timestamp("c.external_fields.crmd_earliesttcenddateupdate").alias("earliesttcenddateupdate"),
            F.col("c.external_fields.crmd_finalizedrate").cast("decimal(38,6)").alias("finalizedrate"),
            F.col("c.external_fields.crmd_hull").alias("crmd_hull"),
            F.col("c.external_fields.crmd_name").alias("crmd_name"),
            F.col("c.external_fields.fixture_id").alias("fixture_id"),
            F.col("c.external_fields.rate_id").alias("rate_id"),
            F.col("c.external_fields.statuscode").cast("long").alias("statuscode"),
            F.col("c.external_fields.statuscodeName").alias("statuscodeName"),
            # --- Metadata ---
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )

    if opt_df.count() == 0:
        log.warning("[silver] ANK_charterparties_option — no option contracts found in bronze")
    else:
        # --- Resolve active rate per contract ---
        rates_opt_df = (
            spark.table(SILVER_CHARTERPARTIES_RATES)
            .select(
                F.col("contractId"),
                F.col("rate").cast("double").alias("rate"),
                F.col("valid_from"),
                F.col("currency"),
            )
            .join(opt_df.select("contractId", "deliveryDate", "extensionDate", "period"), on="contractId", how="inner")
            .withColumn(
                "valid_from_resolved",
                F.when(F.col("valid_from").isNull(), F.coalesce("deliveryDate", "extensionDate"))
                 .otherwise(F.to_timestamp("valid_from")),
            )
        )

        w_desc = W.partitionBy("contractId", "period").orderBy(F.col("valid_from_resolved").desc())
        w_asc  = W.partitionBy("contractId", "period").orderBy(F.col("valid_from_resolved").asc())

        filtered_rates_opt_df = (
            rates_opt_df
            .filter(
                (F.col("period") == "past")
                | ((F.col("period") == "ongoing") & (F.col("valid_from_resolved") <= F.current_timestamp()))
                | (F.col("period") == "upcoming")
            )
            .withColumn("rn", F.row_number().over(w_desc))
            .withColumn("rn1", F.row_number().over(w_asc))
        )

        finalized_rate_opt_df = (
            filtered_rates_opt_df
            .filter(
                ((F.col("period").isin("past", "ongoing")) & (F.col("rn") == 1))
                | ((F.col("period") == "upcoming") & (F.col("rn1") == 1))
            )
            .select(
                F.col("contractId"),
                F.col("rate").alias("final_rate"),
                F.col("valid_from_resolved").cast("timestamp").alias("rate_start"),
                F.col("currency").alias("rate_currency"),
            )
        )

        # --- Final join: add rate columns ---
        result_opt_df = opt_df.join(finalized_rate_opt_df, on="contractId", how="left")

        opt_row_count = result_opt_df.count()
        (
            result_opt_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(SILVER_CHARTERPARTIES_OPTION)
        )
        log.info(f"[silver] ANK_charterparties_option — {opt_row_count} row(s) written → {SILVER_CHARTERPARTIES_OPTION}")

# COMMAND ----------

# DBTITLE 1,Normalize Charterparties All
# ---------------------------------------------------------------------------
# Normalize: silver charterparties_all
# Unions SILVER_CHARTERPARTIES (charterparty + extension with rates) with
# declared option contracts from SILVER_CHARTERPARTIES_OPTION.
# Applies post_hook: NULLs external fields for extension rows.
# ---------------------------------------------------------------------------

# --- 1. Read charterparties combined (charterparty + extension, already has rates) ---
cp_df = spark.table(SILVER_CHARTERPARTIES).withColumnRenamed("Vessel_IMO", "shipImoNr").withColumn("shipImoNr", F.col("shipImoNr").cast("string"))

# --- 2. Read option contracts — only declared ---
opt_declared_df = (
    spark.table(SILVER_CHARTERPARTIES_OPTION)
    .filter(F.col("declarationType") == "declared")
    .withColumn(
        "expectedRedeliveryDate",
        F.when(F.col("toDateIsPlanned") == 1, F.col("redeliveryDate")),
    )
)

_vessel_df = spark.table(VESSEL_TABLE).select(
    F.col("Hull_no"),
    F.col("imo").alias("_v_imo"),
)
opt_declared_df = (
    opt_declared_df
    .join(_vessel_df, on=(F.col("shipImoNr").cast("string") == F.col("Hull_no")), how="left")
    .withColumn("shipImoNr", F.coalesce(F.col("_v_imo").cast("string"), F.col("shipImoNr").cast("string")))
    .drop("Hull_no", "_v_imo")
)

# --- 3. Union all ---
all_df = cp_df.unionByName(opt_declared_df, allowMissingColumns=True)

# --- 4. Apply post_hook: NULL out external fields for extension rows ---
ext_null_cols = [
    "crmd_hull", "crmd_commission", "rate_id", "account_id",
    "fixture_id", "statuscode", "statuscodeName", "finalizedrate",
    "earliesttcenddateupdate",
]
all_df = all_df.withColumns(
    {
        col_name: F.when(F.col("contractType") == "extension", F.lit(None))
                   .otherwise(F.col(col_name))
        for col_name in ext_null_cols
    }
)

# --- 5. Write to silver ---
all_row_count = all_df.count()
(
    all_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_CHARTERPARTIES_ALL)
)
log.info(f"[silver] ANK_charterparties_all — {all_row_count} row(s) written → {SILVER_CHARTERPARTIES_ALL}")

# COMMAND ----------

# DBTITLE 1,Normalize Charterparty Brokerage Commission
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_charterparties → silver (charterparty_brokerageCommission)
# Double-explode: contracts[] → brokerageCommission[] per contract.
# ---------------------------------------------------------------------------

bronze_bkc_df = (
    spark.table(BRONZE_CHARTERPARTIES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_bkc_df.count() == 0:
    log.warning("[silver] ANK_charterparty_brokerageCommission — no pending bronze rows to normalize")
else:
    bkc_df = (
        bronze_bkc_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _cp_brokerage_commission_schema))
        .select(
            F.explode(F.col("_parsed.contracts")).alias("c"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("c.contractId").alias("contractId"),
            F.explode(F.col("c.brokerageCommission")).alias("data"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("contractId"),
            F.col("data.amount").alias("amount"),
            F.col("data.end_date").alias("end_date"),
            F.col("data.broker_id").alias("broker_id"),
            F.col("data.absolute_payment").alias("absolute_payment"),
            F.col("data.ceiling_currency").alias("ceiling_currency"),
            F.col("data.deduct_from_hire").alias("deduct_from_hire"),
            F.col("data.absolute_payment_currency").alias("absolute_payment_currency"),
            F.col("data.absolute_payment_rate_billing_period").alias("absolute_payment_rate_billing_period"),
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )

    bkc_row_count = bkc_df.count()
    if bkc_row_count == 0:
        log.warning("[silver] ANK_charterparty_brokerageCommission — no brokerage commission data found")
    else:
        (
            bkc_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(SILVER_CP_BROKERAGE_COMMISSION)
        )
        log.info(f"[silver] ANK_charterparty_brokerageCommission — {bkc_row_count} row(s) written → {SILVER_CP_BROKERAGE_COMMISSION}")

# COMMAND ----------

# DBTITLE 1,Normalize Charterparty Delivery Notices
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_charterparties → silver (charterparty_deliveryNotices)
# Double-explode: contracts[] → deliveryNotices[] per contract.
# ---------------------------------------------------------------------------

bronze_dn_df = (
    spark.table(BRONZE_CHARTERPARTIES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_dn_df.count() == 0:
    log.warning("[silver] ANK_charterparty_deliveryNotices — no pending bronze rows to normalize")
else:
    dn_df = (
        bronze_dn_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _cp_notices_schema))
        .select(
            F.explode(F.col("_parsed.contracts")).alias("c"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("c.contractId").alias("contractId"),
            F.explode(F.col("c.deliveryNotices")).alias("dn"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("contractId"),
            F.col("dn.nr_days").alias("nr_days"),
            F.col("dn.date_to_tender").alias("date_to_tender"),
            F.col("dn.date_to_tender_formatted").alias("date_to_tender_formatted"),
            F.col("dn.is_definite").alias("is_definite"),
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )

    dn_row_count = dn_df.count()
    if dn_row_count == 0:
        log.warning("[silver] ANK_charterparty_deliveryNotices — no delivery notices found")
    else:
        (
            dn_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(SILVER_CP_DELIVERY_NOTICES)
        )
        log.info(f"[silver] ANK_charterparty_deliveryNotices — {dn_row_count} row(s) written → {SILVER_CP_DELIVERY_NOTICES}")

# COMMAND ----------

# DBTITLE 1,Normalize Charterparty Premiums
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_charterparties → silver (charterparty_premiums)
# Double-explode: contracts[] → premiums[] per contract.
# ---------------------------------------------------------------------------

bronze_prm_df = (
    spark.table(BRONZE_CHARTERPARTIES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_prm_df.count() == 0:
    log.warning("[silver] ANK_charterparty_premiums — no pending bronze rows to normalize")
else:
    prm_df = (
        bronze_prm_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _cp_premiums_schema))
        .select(
            F.explode(F.col("_parsed.contracts")).alias("c"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("c.contractId").alias("contractId"),
            F.explode(F.col("c.premiums")).alias("prm"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("contractId"),
            F.col("prm.start").alias("start"),
            F.col("prm.end").alias("end"),
            F.col("prm.type").alias("type"),
            F.col("prm.uuid").alias("uuid"),
            F.col("prm.amount").alias("amount"),
            F.col("prm.comment").alias("comment"),
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )

    prm_row_count = prm_df.count()
    if prm_row_count == 0:
        log.warning("[silver] ANK_charterparty_premiums — no premiums data found")
    else:
        (
            prm_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(SILVER_CP_PREMIUMS)
        )
        log.info(f"[silver] ANK_charterparty_premiums — {prm_row_count} row(s) written → {SILVER_CP_PREMIUMS}")

# COMMAND ----------

# DBTITLE 1,Normalize Charterparty Redelivery Notices
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_charterparties → silver (charterparty_redeliveryNotices)
# Double-explode: contracts[] → redeliveryNotices[] per contract.
# ---------------------------------------------------------------------------

bronze_rdn_df = (
    spark.table(BRONZE_CHARTERPARTIES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_rdn_df.count() == 0:
    log.warning("[silver] ANK_charterparty_redeliveryNotices — no pending bronze rows to normalize")
else:
    rdn_df = (
        bronze_rdn_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _cp_notices_schema))
        .select(
            F.explode(F.col("_parsed.contracts")).alias("c"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("c.contractId").alias("contractId"),
            F.explode(F.col("c.redeliveryNotices")).alias("rdn"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("contractId"),
            F.col("rdn.nr_days").alias("nr_days"),
            F.col("rdn.date_to_tender").alias("date_to_tender"),
            F.col("rdn.date_to_tender_formatted").alias("date_to_tender_formatted"),
            F.col("rdn.is_definite").alias("is_definite"),
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )

    rdn_row_count = rdn_df.count()
    if rdn_row_count == 0:
        log.warning("[silver] ANK_charterparty_redeliveryNotices — no redelivery notices found")
    else:
        (
            rdn_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(SILVER_CP_REDELIVERY_NOTICES)
        )
        log.info(f"[silver] ANK_charterparty_redeliveryNotices — {rdn_row_count} row(s) written → {SILVER_CP_REDELIVERY_NOTICES}")

# COMMAND ----------

# DBTITLE 1,Normalize Offhires
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_offhires → silver (offhires)
# Single explode: RawData is a top-level JSON array of offhire objects.
# ---------------------------------------------------------------------------

bronze_oh_df = (
    spark.table(BRONZE_OFFHIRES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_oh_df.count() == 0:
    log.warning("[silver] ANK_offhires — no pending bronze rows to normalize")
else:
    oh_df = (
        bronze_oh_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _offhires_schema))
        .select(
            F.explode(F.col("_parsed")).alias("o"),
            F.col("_Batch_run_id"),
        )
        .select(
            # --- Flat fields ---
            F.col("o.actual_settlement_amount").alias("actual_settlement_amount"),
            F.col("o.add_offhire_duration_to_charter_period").alias("add_offhire_duration_to_charter_period"),
            F.col("o.add_offhire_duration_to_charter_period_comment").alias("add_offhire_duration_to_charter_period_comment"),
            F.col("o.address_commission").alias("address_commission"),
            F.col("o.address_commission_amount").alias("address_commission_amount"),
            # --- Broker (nested) ---
            F.col("o.broker.full_name").alias("broker_full_name"),
            F.col("o.broker.id").alias("broker_id"),
            F.col("o.broker.initials").alias("broker_initials"),
            F.col("o.broker.primary_color").alias("broker_primary_color"),
            F.col("o.broker.short_name").alias("broker_short_name"),
            # --- Brokerage ---
            F.col("o.brokerage_commission").alias("brokerage_commission"),
            F.col("o.brokerage_commission_amount_total").alias("brokerage_commission_amount_total"),
            # --- Bunker amounts (cast to decimal) ---
            F.col("o.bunker_amounts.lng").cast("decimal(38,6)").alias("bunker_amounts_lng"),
            F.col("o.bunker_amounts.vlsfo").cast("decimal(38,6)").alias("bunker_amounts_vlsfo"),
            F.col("o.bunker_amounts.ulsmgo").cast("decimal(38,6)").alias("bunker_amounts_ulsmgo"),
            F.col("o.bunker_amounts.lsmgo").cast("decimal(38,6)").alias("bunker_amounts_lsmgo"),
            F.col("o.bunker_amounts.ulsfo").cast("decimal(38,6)").alias("bunker_amounts_ulsfo"),
            F.col("o.bunker_amounts.hfo").cast("decimal(38,6)").alias("bunker_amounts_hfo"),
            F.col("o.bunker_amounts.hsfo").cast("decimal(38,6)").alias("bunker_amounts_hsfo"),
            # --- Bunker / charter ---
            F.col("o.bunker_value").alias("bunker_value"),
            F.col("o.charter_direction").alias("charter_direction"),
            F.col("o.charter_party_id").alias("charter_party_id"),
            # --- Charterer (nested) ---
            F.col("o.charterer.full_name").alias("charterer_full_name"),
            F.col("o.charterer.id").alias("charterer_id"),
            F.col("o.charterer.initials").alias("charterer_initials"),
            F.col("o.charterer.primary_color").alias("charterer_primary_color"),
            F.col("o.charterer.short_name").alias("charterer_short_name"),
            # --- CII ---
            F.col("o.cii_clause").alias("cii_clause"),
            F.col("o.cii_clause_rating_at_delivery").alias("cii_clause_rating_at_delivery"),
            F.col("o.cii_clause_rating_at_redelivery").alias("cii_clause_rating_at_redelivery"),
            # --- Contract ---
            F.col("o.comments").alias("comments"),
            F.to_timestamp("o.contract_date").alias("contract_date"),
            F.col("o.contract_id").alias("contract_id"),
            F.col("o.contract_id_internal").alias("contract_id_internal"),
            F.col("o.contract_status_ready").alias("contract_status_ready"),
            F.col("o.contract_type").alias("contract_type"),
            F.col("o.contract_type_text").alias("contract_type_text"),
            F.col("o.currency").alias("currency"),
            F.col("o.damage_reports_count").alias("damage_reports_count"),
            F.col("o.duration").alias("duration"),
            # --- ETS ---
            F.col("o.ets_clause").alias("ets_clause"),
            F.col("o.ets_comment").alias("ets_comment"),
            F.col("o.ets_notify_charterer_within").alias("ets_notify_charterer_within"),
            F.col("o.ets_option_to_settle_eua_in_cash").alias("ets_option_to_settle_eua_in_cash"),
            F.col("o.ets_option_to_settle_eua_in_cash_period").alias("ets_option_to_settle_eua_in_cash_period"),
            F.col("o.ets_payment_fulfilment").alias("ets_payment_fulfilment"),
            F.col("o.ets_reporting_period").alias("ets_reporting_period"),
            F.col("o.ets_reporting_period_split").alias("ets_reporting_period_split"),
            F.col("o.ets_settlement_redelivery").alias("ets_settlement_redelivery"),
            F.col("o.ets_settlement_redelivery_days").alias("ets_settlement_redelivery_days"),
            F.col("o.ets_transfer_of_eua_to_owner").alias("ets_transfer_of_eua_to_owner"),
            F.col("o.ets_transfer_of_eua_to_owner_date").alias("ets_transfer_of_eua_to_owner_date"),
            F.col("o.ets_transfer_of_eua_to_owner_numeric").alias("ets_transfer_of_eua_to_owner_numeric"),
            # --- File / fixer ---
            F.col("o.file_count").alias("file_count"),
            F.col("o.fixer.initials").alias("fixer_initials"),
            # --- Dates (cast to timestamp) ---
            F.to_timestamp("o.from_date").alias("from_date"),
            F.col("o.from_date_is_planned").alias("from_date_is_planned"),
            F.col("o.from_date_timezone").alias("from_date_timezone"),
            # --- Fuel EU ---
            F.col("o.fuel_eu_allowed_to_bank").alias("fuel_eu_allowed_to_bank"),
            F.col("o.fuel_eu_allowed_to_bank_notify_by").alias("fuel_eu_allowed_to_bank_notify_by"),
            F.col("o.fuel_eu_allowed_to_bank_reporting_period").alias("fuel_eu_allowed_to_bank_reporting_period"),
            F.col("o.fuel_eu_allowed_to_borrow").alias("fuel_eu_allowed_to_borrow"),
            F.col("o.fuel_eu_allowed_to_borrow_notify_by").alias("fuel_eu_allowed_to_borrow_notify_by"),
            F.col("o.fuel_eu_allowed_to_borrow_reimburse_charterers_for_paid_surcharge_days_after").alias("fuel_eu_allowed_to_borrow_reimburse_charterers_for_paid_surcharge_days_after"),
            F.col("o.fuel_eu_allowed_to_borrow_reporting_period").alias("fuel_eu_allowed_to_borrow_reporting_period"),
            F.col("o.fuel_eu_allowed_to_pool").alias("fuel_eu_allowed_to_pool"),
            F.col("o.fuel_eu_allowed_to_pool_notify_by").alias("fuel_eu_allowed_to_pool_notify_by"),
            F.col("o.fuel_eu_allowed_to_pool_reimburse_charterers_for_paid_surcharge_days_after").alias("fuel_eu_allowed_to_pool_reimburse_charterers_for_paid_surcharge_days_after"),
            F.col("o.fuel_eu_allowed_to_pool_reporting_period").alias("fuel_eu_allowed_to_pool_reporting_period"),
            F.col("o.fuel_eu_clause").alias("fuel_eu_clause"),
            F.col("o.fuel_eu_comment").alias("fuel_eu_comment"),
            F.col("o.fuel_eu_compliance_balance_at_delivery").alias("fuel_eu_compliance_balance_at_delivery"),
            F.col("o.fuel_eu_compliance_balance_at_delivery_date").alias("fuel_eu_compliance_balance_at_delivery_date"),
            F.col("o.fuel_eu_compliance_balance_at_previous_reporting_period").alias("fuel_eu_compliance_balance_at_previous_reporting_period"),
            F.col("o.fuel_eu_compliance_balance_at_previous_reporting_period_year").alias("fuel_eu_compliance_balance_at_previous_reporting_period_year"),
            F.col("o.fuel_eu_delivery_agreement").alias("fuel_eu_delivery_agreement"),
            F.col("o.fuel_eu_delivery_agreement_negative_compliance_balance_currency").alias("fuel_eu_delivery_agreement_negative_compliance_balance_currency"),
            F.col("o.fuel_eu_delivery_agreement_negative_compliance_balance_max").alias("fuel_eu_delivery_agreement_negative_compliance_balance_max"),
            F.col("o.fuel_eu_delivery_agreement_negative_compliance_balance_per_tonne").alias("fuel_eu_delivery_agreement_negative_compliance_balance_per_tonne"),
            F.col("o.fuel_eu_delivery_agreement_positive_compliance_balance_currency").alias("fuel_eu_delivery_agreement_positive_compliance_balance_currency"),
            F.col("o.fuel_eu_delivery_agreement_positive_compliance_balance_max").alias("fuel_eu_delivery_agreement_positive_compliance_balance_max"),
            F.col("o.fuel_eu_delivery_agreement_positive_compliance_balance_per_tonne").alias("fuel_eu_delivery_agreement_positive_compliance_balance_per_tonne"),
            F.col("o.fuel_eu_notify_charterer_within").alias("fuel_eu_notify_charterer_within"),
            F.col("o.fuel_eu_reference_code").alias("fuel_eu_reference_code"),
            F.col("o.fuel_eu_reimburse_charterers_for_positive_compliance_balance").alias("fuel_eu_reimburse_charterers_for_positive_compliance_balance"),
            F.col("o.fuel_eu_reimburse_charterers_for_positive_compliance_balance_currency").alias("fuel_eu_reimburse_charterers_for_positive_compliance_balance_currency"),
            F.col("o.fuel_eu_reimburse_charterers_for_positive_compliance_balance_max").alias("fuel_eu_reimburse_charterers_for_positive_compliance_balance_max"),
            F.col("o.fuel_eu_reimburse_charterers_for_positive_compliance_balance_paid_by").alias("fuel_eu_reimburse_charterers_for_positive_compliance_balance_paid_by"),
            F.col("o.fuel_eu_reimburse_charterers_for_positive_compliance_balance_per_tonne").alias("fuel_eu_reimburse_charterers_for_positive_compliance_balance_per_tonne"),
            F.col("o.fuel_eu_reimburse_owner_for_penalty_multiplier").alias("fuel_eu_reimburse_owner_for_penalty_multiplier"),
            F.col("o.fuel_eu_reimburse_owner_for_penalty_multiplier_amount").alias("fuel_eu_reimburse_owner_for_penalty_multiplier_amount"),
            F.col("o.fuel_eu_reimburse_owner_for_penalty_multiplier_currency").alias("fuel_eu_reimburse_owner_for_penalty_multiplier_currency"),
            F.col("o.fuel_eu_reimburse_owner_for_penalty_multiplier_paid_by").alias("fuel_eu_reimburse_owner_for_penalty_multiplier_paid_by"),
            F.col("o.fuel_eu_reporting_period").alias("fuel_eu_reporting_period"),
            F.col("o.fuel_eu_surcharge_settlement").alias("fuel_eu_surcharge_settlement"),
            # --- Hire (cast to float) ---
            F.col("o.hire_value").cast("double").alias("hire_value"),
            # --- Flags ---
            F.col("o.is_last_in_series").alias("is_last_in_series"),
            F.col("o.is_planned").alias("is_planned"),
            F.col("o.last_rate").alias("last_rate"),
            F.col("o.last_rate_currency").alias("last_rate_currency"),
            F.col("o.location").alias("location"),
            F.col("o.ls_value").alias("ls_value"),
            F.col("o.managed_in_pool").alias("managed_in_pool"),
            F.col("o.managed_in_pool_comment").alias("managed_in_pool_comment"),
            # --- Offhire dates (cast to timestamp) ---
            F.to_timestamp("o.offhire_date").alias("offhire_date"),
            F.col("o.offhire_ratio").cast("double").alias("offhire_ratio"),
            F.col("o.offhire_uuid").alias("offhire_uuid"),
            F.col("o.offhires_count").alias("offhires_count"),
            F.col("o.offhires_duration").alias("offhires_duration"),
            F.to_timestamp("o.onhire_date").alias("onhire_date"),
            F.col("o.onhire_duration").alias("onhire_duration"),
            # --- Option ---
            F.col("o.option_declaration_type").alias("option_declaration_type"),
            F.col("o.option_declaration_type_text").alias("option_declaration_type_text"),
            F.col("o.option_to_add_offhire_days").alias("option_to_add_offhire_days"),
            F.col("o.option_to_add_offhire_days_comment").alias("option_to_add_offhire_days_comment"),
            F.col("o.option_type").alias("option_type"),
            F.to_timestamp("o.order_date").alias("order_date"),
            # --- Owner (nested) ---
            F.col("o.owner.full_name").alias("owner_full_name"),
            F.col("o.owner.id").alias("owner_id"),
            F.col("o.owner.initials").alias("owner_initials"),
            F.col("o.owner.primary_color").alias("owner_primary_color"),
            F.col("o.owner.short_name").alias("owner_short_name"),
            # --- Period / postfixer ---
            F.col("o.period").alias("period"),
            F.col("o.postfixer.email").alias("postfixer_email"),
            F.col("o.postfixer.enabled").alias("postfixer_enabled"),
            F.col("o.postfixer.id").alias("postfixer_id"),
            F.col("o.postfixer.initials").alias("postfixer_initials"),
            F.col("o.postfixer.username").alias("postfixer_username"),
            # --- Rate ---
            F.col("o.rate_type").alias("rate_type"),
            F.col("o.rate_type_label").alias("rate_type_label"),
            F.col("o.reason").alias("reason"),
            # --- Redelivery dates (cast to timestamp) ---
            F.to_timestamp("o.redelivery_max_date").alias("redelivery_max_date"),
            F.to_timestamp("o.redelivery_min_date").alias("redelivery_min_date"),
            F.col("o.redelivery_period").alias("redelivery_period"),
            F.col("o.redelivery_plan").alias("redelivery_plan"),
            F.col("o.redelivery_range").alias("redelivery_range"),
            # --- Ship ---
            F.col("o.ship_archived").alias("ship_archived"),
            F.col("o.ship_id").alias("ship_id"),
            F.col("o.ship_imo_nr").alias("ship_imo_nr"),
            F.col("o.ship_name").alias("ship_name"),
            F.col("o.ship_nominal_capacity").alias("ship_nominal_capacity"),
            # --- Status / to_date ---
            F.col("o.status").alias("status"),
            F.col("o.to_count").alias("to_count"),
            F.to_timestamp("o.to_date").alias("to_date"),
            F.col("o.to_date_is_planned").alias("to_date_is_planned"),
            F.col("o.to_date_timezone").alias("to_date_timezone"),
            F.col("o.total_value").alias("total_value"),
            # --- Metadata ---
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )

    oh_row_count = oh_df.count()
    if oh_row_count == 0:
        log.warning("[silver] ANK_offhires — no offhire records found")
    else:
        (
            oh_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(SILVER_OFFHIRES)
        )
        log.info(f"[silver] ANK_offhires — {oh_row_count} row(s) written → {SILVER_OFFHIRES}")

# COMMAND ----------

# DBTITLE 1,Normalize Offhires At Cost
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_offhires → silver (offhires_atCost)
# Double-explode: offhires[] → atCost[] per offhire.
# ---------------------------------------------------------------------------

bronze_ohatc_df = (
    spark.table(BRONZE_OFFHIRES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_ohatc_df.count() == 0:
    log.warning("[silver] ANK_offhires_atCost — no pending bronze rows to normalize")
else:
    ohatc_df = (
        bronze_ohatc_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _offhires_schema))
        .select(
            F.explode(F.col("_parsed")).alias("o"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("o.offhire_uuid").alias("offhire_uuid"),
            F.explode(F.col("o.atCost")).alias("atc"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("offhire_uuid"),
            F.col("atc.at_cost_type").alias("at_cost_type"),
            F.col("atc.at_cost_unit").alias("at_cost_unit"),
            F.col("atc.at_cost_amount").alias("at_cost_amount"),
            F.col("atc.at_cost_comment").alias("at_cost_comment"),
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )

    ohatc_row_count = ohatc_df.count()
    if ohatc_row_count == 0:
        log.warning("[silver] ANK_offhires_atCost — no at-cost data found")
    else:
        (
            ohatc_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(SILVER_OFFHIRES_ATCOST)
        )
        log.info(f"[silver] ANK_offhires_atCost — {ohatc_row_count} row(s) written → {SILVER_OFFHIRES_ATCOST}")

# COMMAND ----------

# DBTITLE 1,Normalize Offhire Lump Sums
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_offhires → silver (offhire_lumpSums)
# Triple-explode: offhires[] → rates[] → lump_sums[] per rate.
# ---------------------------------------------------------------------------

bronze_ohls_df = (
    spark.table(BRONZE_OFFHIRES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_ohls_df.count() == 0:
    log.warning("[silver] ANK_offhire_lumpSums — no pending bronze rows to normalize")
else:
    ohls_df = (
        bronze_ohls_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _offhires_schema))
        .select(
            F.explode(F.col("_parsed")).alias("o"),
            F.col("_Batch_run_id"),
        )
        # First explode: rates per offhire
        .select(
            F.col("o.offhire_uuid").alias("offhire_uuid"),
            F.explode(F.col("o.rates")).alias("rate"),
            F.col("_Batch_run_id"),
        )
        # Second explode: lump_sums per rate
        .select(
            F.col("offhire_uuid"),
            F.explode(F.col("rate.lump_sums")).alias("ls"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("offhire_uuid"),
            F.col("ls.type").alias("type"),
            F.col("ls.ls_unit").alias("ls_unit"),
            F.col("ls.ls_amount").alias("ls_amount"),
            F.col("ls.ls_currency").alias("ls_currency"),
            F.col("ls.rate_number").alias("rate_number"),
            F.col("ls.lump_sum_uuid").alias("lump_sum_uuid"),
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )

    ohls_row_count = ohls_df.count()
    if ohls_row_count == 0:
        log.warning("[silver] ANK_offhire_lumpSums — no lump sums data found")
    else:
        (
            ohls_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(SILVER_OFFHIRES_LUMPSUMS)
        )
        log.info(f"[silver] ANK_offhire_lumpSums — {ohls_row_count} row(s) written → {SILVER_OFFHIRES_LUMPSUMS}")

# COMMAND ----------

# DBTITLE 1,Normalize Offhire Rates
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_offhires → silver (offhire_rates)
# Double-explode: offhires[] → rates[] per offhire.
# ---------------------------------------------------------------------------

bronze_ohr_df = (
    spark.table(BRONZE_OFFHIRES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_ohr_df.count() == 0:
    log.warning("[silver] ANK_offhire_rates — no pending bronze rows to normalize")
else:
    ohr_df = (
        bronze_ohr_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _offhires_schema))
        .select(
            F.explode(F.col("_parsed")).alias("o"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("o.offhire_uuid").alias("offhire_uuid"),
            F.explode(F.col("o.rates")).alias("r"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("offhire_uuid"),
            F.col("r.rate").alias("rate"),
            F.col("r.special").alias("special"),
            F.col("r.currency").alias("currency"),
            F.col("r.rate_uuid").alias("rate_uuid"),
            F.col("r.valid_from").alias("valid_from"),
            F.col("offhire_uuid").alias("lump_sums"),
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )

    ohr_row_count = ohr_df.count()
    if ohr_row_count == 0:
        log.warning("[silver] ANK_offhire_rates — no rates data found")
    else:
        (
            ohr_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(SILVER_OFFHIRES_RATES)
        )
        log.info(f"[silver] ANK_offhire_rates — {ohr_row_count} row(s) written → {SILVER_OFFHIRES_RATES}")

# COMMAND ----------

# DBTITLE 1,Normalize Reviews
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_reviews → silver (reviews)
# Single explode: RawData is a top-level JSON array of review objects.
# ---------------------------------------------------------------------------

bronze_rev_df = (
    spark.table(BRONZE_REVIEWS)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_rev_df.count() == 0:
    log.warning("[silver] ANK_reviews — no pending bronze rows to normalize")
else:
    rev_df = (
        bronze_rev_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _reviews_schema))
        .select(
            F.explode(F.col("_parsed")).alias("j"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("j.id").alias("id"),
            F.col("j.uuidhex").alias("uuidhex"),
            F.col("j.contract_id").alias("contract_id"),
            F.col("j.contract_type").alias("contract_type"),
            F.col("j.review_type").alias("review_type"),
            F.col("j.review_type_raw").alias("review_type_raw"),
            F.col("j.contract_date").alias("contract_date"),
            F.col("j.ship_name").alias("ship_name"),
            F.col("j.ship_id").alias("ship_id"),
            F.col("j.ship_imo_nr").alias("ship_imo_nr"),
            F.col("j.number_of_changes").alias("number_of_changes"),
            F.col("j.modified_date").alias("modified_date"),
            F.col("j.modified_by").alias("modified_by"),
            F.col("j.approved_date").alias("approved_date"),
            F.col("j.approval_approved").alias("approval_approved"),
            F.col("j.approved_by").alias("approved_by"),
            F.col("j.fixer").alias("fixer"),
            F.col("j.postfixer").alias("postfixer"),
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )

    rev_row_count = rev_df.count()
    if rev_row_count == 0:
        log.warning("[silver] ANK_reviews — no review records found")
    else:
        (
            rev_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(SILVER_REVIEWS)
        )
        log.info(f"[silver] ANK_reviews — {rev_row_count} row(s) written → {SILVER_REVIEWS}")

# COMMAND ----------

# DBTITLE 1,Normalize Offhire Bunker
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_offhires → silver (offhire_bunker)
# Double-explode: offhires[] → bunker{} (map explode: key = bunker type).
# ---------------------------------------------------------------------------

bronze_ohb_df = (
    spark.table(BRONZE_OFFHIRES)
    .filter(F.col("_is_normalized") == False)
    .orderBy(F.col("_ingestion_ts").desc())
    .limit(1)
)

if bronze_ohb_df.count() == 0:
    log.warning("[silver] ANK_offhire_bunker — no pending bronze rows to normalize")
else:
    ohb_df = (
        bronze_ohb_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _offhires_schema))
        .select(
            F.explode(F.col("_parsed")).alias("o"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("o.offhire_uuid").alias("offhire_uuid"),
            F.explode(F.col("o.bunker")).alias("key", "bunker"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("offhire_uuid"),
            F.col("key"),
            F.when(
                F.col("bunker.roe").cast("string").isin('{"rates":[]}', ""),
                F.lit(None)
            ).otherwise(F.col("bunker.roe")).alias("roe"),
            F.col("bunker.price").cast("double").alias("price"),
            F.col("bunker.based_on").alias("based_on"),
            F.col("bunker.onhire_quantity").cast("double").alias("onhire_quantity"),
            F.col("bunker.offhire_quantity").cast("double").alias("offhire_quantity"),
            F.col("bunker.additional_quantity").alias("additional_quantity"),
            F.col("_Batch_run_id"),
            F.current_timestamp().alias("_ingestion_ts"),
        )
    )

    ohb_row_count = ohb_df.count()
    if ohb_row_count == 0:
        log.warning("[silver] ANK_offhire_bunker — no bunker data found")
    else:
        (
            ohb_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(SILVER_OFFHIRES_BUNKER)
        )
        log.info(f"[silver] ANK_offhire_bunker — {ohb_row_count} row(s) written → {SILVER_OFFHIRES_BUNKER}")

# COMMAND ----------

# DBTITLE 1,Normalize Ships Properties (Pivot)
# ---------------------------------------------------------------------------
# Normalize: bronze ANK_ships_properties → silver (ships_properties)
# Explodes properties array, joins with ship_property_map, then pivots
# crm_field values into columns per shipImoNr.
# ---------------------------------------------------------------------------
def _clean_col_name(name: str) -> str:
    """Replicate dbt Jinja column-name sanitisation."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name.replace(" ", "_")).strip("_")


bronze_sp_all = (
    spark.table(BRONZE_SHIPS_PROPERTIES)
    .filter(F.col("_is_normalized") == False)
)

latest_batch_row = bronze_sp_all.orderBy(F.col("_ingestion_ts").desc()).select("_Batch_run_id").first()

if latest_batch_row is None:
    log.warning("[silver] ANK_ships_properties — no pending bronze rows to normalize")
else:
    latest_batch_id = latest_batch_row["_Batch_run_id"]
    bronze_sp_df = bronze_sp_all.filter(F.col("_Batch_run_id") == latest_batch_id)
    # --- Parse JSON (single object per row, not array) ---
    parsed_df = (
        bronze_sp_df
        .withColumn("_parsed", F.from_json(F.col("Raw_data"), _ships_properties_schema))
        .select(
            F.col("_parsed.uuid").alias("uuid"),
            F.to_timestamp("_parsed.lastUpdated").alias("lastUpdated"),
            F.col("_parsed.shipImoNr").alias("shipImoNr"),
            F.explode(F.col("_parsed.properties")).alias("prop"),
            F.col("_Batch_run_id"),
        )
        .select(
            F.col("uuid"),
            F.col("lastUpdated"),
            F.col("shipImoNr"),
            F.col("prop.id").alias("property_id"),
            F.col("prop.title").alias("title"),
            F.col("prop.value").alias("value"),
            F.col("prop.isVerified").alias("isVerified"),
            F.col("prop.lastUpdated").alias("prop_lastUpdated"),
            F.col("_Batch_run_id"),
        )
    )

    # --- Join with property map to get crm_field ---
    prop_map_df = spark.table(PROPERTY_MAP).select(
        F.col("property_id"),
        F.col("crm_field"),
    )

    base_df = (
        parsed_df
        .join(prop_map_df, on="property_id", how="inner")
        .filter(F.col("title").isNotNull())
    )

    # --- Dynamic pivot: crm_field values become columns ---
    pivot_cols = [
        row.crm_field
        for row in base_df.select("crm_field").distinct().orderBy("crm_field").collect()
    ]

    pivoted_df = (
        base_df
        .groupBy("shipImoNr")
        .pivot("crm_field", pivot_cols)
        .agg(F.max("value"))
    )

    # --- Sanitise column names (replicate dbt Jinja replace chain) ---
    for col_name in pivoted_df.columns:
        clean = _clean_col_name(col_name)
        if clean != col_name:
            pivoted_df = pivoted_df.withColumnRenamed(col_name, clean)

    # --- Add metadata columns ---
    meta_df = (
        base_df
        .groupBy("shipImoNr")
        .agg(
            F.max("uuid").alias("uuid"),
            F.max("lastUpdated").alias("lastUpdated"),
            F.max("prop_lastUpdated").alias("last_property_updated_at"),
            F.max(F.col("isVerified").cast("boolean").cast("int")).alias("any_verified"),
            F.max("_Batch_run_id").alias("_Batch_run_id"),
        )
    )

    result_sp_df = (
        pivoted_df
        .join(meta_df, on="shipImoNr", how="left")
        .withColumn("_ingestion_ts", F.current_timestamp())
    )

    sp_row_count = result_sp_df.count()
    (
        result_sp_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_SHIPS_PROPERTIES)
    )
    log.info(f"[silver] ANK_ships_properties — {sp_row_count} row(s) written → {SILVER_SHIPS_PROPERTIES}")

# COMMAND ----------

# DBTITLE 1,Mark Bronze Normalized
# ---------------------------------------------------------------------------
# Set _is_normalized = True for all bronze tables after silver processing
# ---------------------------------------------------------------------------
for table_name in BRONZE_TABLES.values():
    DeltaTable.forName(spark, table_name).update(
        condition=F.col("_is_normalized") == False,
        set={"_is_normalized": F.lit(True)},
    )
    log.info(f"[bronze] _is_normalized set to True → {table_name}")