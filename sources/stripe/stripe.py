# Stripe connector implementation


import requests
import json
from pyspark.sql.types import *
from datetime import datetime
import time
from typing import Dict, List, Tuple, Iterator, Any


class LakeflowConnect:
    def __init__(self, options: dict) -> None:
        """
        Initialize the Stripe connector with API credentials.

        Args:
            options: Dictionary containing:
                - api_key: Stripe secret API key (sk_test_* or sk_live_*)
        """
        self.api_key = options["api_key"]
        self.base_url = "https://api.stripe.com/v1"
        self.auth = (self.api_key, "")  # API key as username, empty password

        # Cache for schemas to avoid repeated computation
        self._schema_cache = {}

        # Centralized object metadata configuration
        self._object_config = {
            "customers": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "customers",
                "supports_deleted": True
            },
            "charges": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "charges",
                "supports_deleted": False
            },
            "payment_intents": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "payment_intents",
                "supports_deleted": False
            },
            "subscriptions": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "subscriptions",
                "supports_deleted": False
            },
            "invoices": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "invoices",
                "supports_deleted": False
            },
            "products": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "products",
                "supports_deleted": True
            },
            "prices": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "prices",
                "supports_deleted": False
            },
            "refunds": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "refunds",
                "supports_deleted": False
            },
            "disputes": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "disputes",
                "supports_deleted": False
            },
            "payment_methods": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "payment_methods",
                "supports_deleted": False
            },
            "balance_transactions": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "balance_transactions",
                "supports_deleted": False
            },
            "payouts": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "payouts",
                "supports_deleted": False
            },
            "invoice_items": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "invoiceitems",
                "supports_deleted": False
            },
            "plans": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "plans",
                "supports_deleted": True
            },
            "events": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "events",
                "supports_deleted": False
            },
            "coupons": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "coupons",
                "supports_deleted": False
            }
        }

    def list_tables(self) -> list[str]:
        """
        List available Stripe tables/objects.

        Returns:
            List of supported table names
        """
        return [
            "customers",
            "charges",
            "payment_intents",
            "subscriptions",
            "invoices",
            "products",
            "prices",
            "refunds",
            "disputes",
            "payment_methods",
            "balance_transactions",
            "payouts",
            "invoice_items",
            "plans",
            "events",
            "coupons"
        ]

    def get_table_schema(self, table_name: str) -> StructType:
        """
        Get the Spark schema for a Stripe table.

        Args:
            table_name: Name of the table

        Returns:
            StructType representing the table schema
        """
        schema, _ = self._get_table_details(table_name)
        return schema

    def read_table_metadata(self, table_name: str) -> dict:
        """
        Get metadata for a Stripe table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with primary_key, cursor_field, and ingestion_type
        """
        _, metadata = self._get_table_details(table_name)
        return {
            "primary_key": metadata["primary_key"],
            "cursor_field": metadata["cursor_field"],
            "ingestion_type": metadata["ingestion_type"]
        }

    def read_table(self, table_name: str, start_offset: dict) -> Tuple[List[Dict], Dict]:
        """
        Read data from a Stripe table.

        Args:
            table_name: Name of the table to read
            start_offset: Dictionary containing cursor information for incremental reads
                - For incremental: {"created": <unix_timestamp>}
                - For full refresh: None or {}

        Returns:
            Tuple of (records, new_offset)
        """
        if table_name not in self._object_config:
            raise ValueError(f"Unsupported table: {table_name}")

        config = self._object_config[table_name]

        # Determine if this is an incremental read
        is_incremental = start_offset is not None and start_offset.get(config["cursor_field"]) is not None

        if is_incremental:
            return self._read_data_incremental(table_name, start_offset)
        else:
            return self._read_data_full(table_name)

    def _get_table_details(self, table_name: str) -> Tuple[StructType, Dict]:
        """
        Get schema and metadata for a table.

        Args:
            table_name: Name of the table

        Returns:
            Tuple of (schema, metadata)
        """
        # Check cache first
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        # Generate schema based on table type
        if table_name == "customers":
            schema, metadata = self._get_customers_schema()
        elif table_name == "charges":
            schema, metadata = self._get_charges_schema()
        elif table_name == "payment_intents":
            schema, metadata = self._get_payment_intents_schema()
        elif table_name == "subscriptions":
            schema, metadata = self._get_subscriptions_schema()
        elif table_name == "invoices":
            schema, metadata = self._get_invoices_schema()
        elif table_name == "products":
            schema, metadata = self._get_products_schema()
        elif table_name == "prices":
            schema, metadata = self._get_prices_schema()
        elif table_name == "refunds":
            schema, metadata = self._get_refunds_schema()
        elif table_name == "disputes":
            schema, metadata = self._get_disputes_schema()
        elif table_name == "payment_methods":
            schema, metadata = self._get_payment_methods_schema()
        elif table_name == "balance_transactions":
            schema, metadata = self._get_balance_transactions_schema()
        elif table_name == "payouts":
            schema, metadata = self._get_payouts_schema()
        elif table_name == "invoice_items":
            schema, metadata = self._get_invoice_items_schema()
        elif table_name == "plans":
            schema, metadata = self._get_plans_schema()
        elif table_name == "events":
            schema, metadata = self._get_events_schema()
        elif table_name == "coupons":
            schema, metadata = self._get_coupons_schema()
        else:
            raise ValueError(f"Unsupported table: {table_name}")

        # Cache the result
        self._schema_cache[table_name] = (schema, metadata)

        return schema, metadata

    def _get_customers_schema(self) -> Tuple[StructType, Dict]:
        """
        Define schema for the customers table.
        Based on Stripe's Customer object structure.

        Returns:
            Tuple of (schema, metadata)
        """
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),  # Primary key, not nullable
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),  # Unix timestamp
            StructField("updated", LongType(), True),  # Unix timestamp (if available)
            StructField("livemode", BooleanType(), True),

            # Customer details
            StructField("email", StringType(), True),
            StructField("name", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("description", StringType(), True),

            # Address (flattened)
            StructField("address_line1", StringType(), True),
            StructField("address_line2", StringType(), True),
            StructField("address_city", StringType(), True),
            StructField("address_state", StringType(), True),
            StructField("address_postal_code", StringType(), True),
            StructField("address_country", StringType(), True),

            # Shipping (flattened)
            StructField("shipping_name", StringType(), True),
            StructField("shipping_phone", StringType(), True),
            StructField("shipping_address_line1", StringType(), True),
            StructField("shipping_address_line2", StringType(), True),
            StructField("shipping_address_city", StringType(), True),
            StructField("shipping_address_state", StringType(), True),
            StructField("shipping_address_postal_code", StringType(), True),
            StructField("shipping_address_country", StringType(), True),

            # Financial fields
            StructField("balance", LongType(), True),  # In cents
            StructField("currency", StringType(), True),
            StructField("delinquent", BooleanType(), True),

            # Preferences
            StructField("preferred_locales", ArrayType(StringType()), True),

            # Invoice settings (as JSON string)
            StructField("invoice_settings", StringType(), True),

            # Tax information
            StructField("tax_exempt", StringType(), True),  # Enum: none, exempt, reverse

            # References to other objects
            StructField("default_source", StringType(), True),
            StructField("invoice_prefix", StringType(), True),

            # Metadata (as JSON string)
            StructField("metadata", StringType(), True),

            # Discount information
            StructField("discount", StringType(), True),  # As JSON string

            # Deletion tracking
            StructField("deleted", BooleanType(), True)
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": True,
            "object_type": "customer"
        }

        return schema, metadata

    def _get_charges_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the charges table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Amount fields
            StructField("amount", LongType(), True),  # In cents
            StructField("amount_captured", LongType(), True),
            StructField("amount_refunded", LongType(), True),
            StructField("currency", StringType(), True),

            # Status and outcome
            StructField("status", StringType(), True),  # succeeded, pending, failed
            StructField("paid", BooleanType(), True),
            StructField("refunded", BooleanType(), True),
            StructField("captured", BooleanType(), True),
            StructField("disputed", BooleanType(), True),

            # Related objects
            StructField("customer", StringType(), True),
            StructField("invoice", StringType(), True),
            StructField("payment_intent", StringType(), True),
            StructField("payment_method", StringType(), True),

            # Transaction details
            StructField("description", StringType(), True),
            StructField("receipt_email", StringType(), True),
            StructField("receipt_url", StringType(), True),
            StructField("statement_descriptor", StringType(), True),

            # Billing details (as JSON string)
            StructField("billing_details", StringType(), True),
            StructField("payment_method_details", StringType(), True),
            StructField("outcome", StringType(), True),

            # Metadata
            StructField("metadata", StringType(), True),

            # Failure information
            StructField("failure_code", StringType(), True),
            StructField("failure_message", StringType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": False,
            "object_type": "charge"
        }

        return schema, metadata

    def _get_payment_intents_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the payment_intents table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Amount fields
            StructField("amount", LongType(), True),
            StructField("amount_capturable", LongType(), True),
            StructField("amount_received", LongType(), True),
            StructField("currency", StringType(), True),

            # Status
            StructField("status", StringType(), True),  # requires_payment_method, requires_confirmation, etc.
            StructField("canceled_at", LongType(), True),
            StructField("cancellation_reason", StringType(), True),

            # Related objects
            StructField("customer", StringType(), True),
            StructField("invoice", StringType(), True),
            StructField("payment_method", StringType(), True),

            # Transaction details
            StructField("description", StringType(), True),
            StructField("receipt_email", StringType(), True),
            StructField("statement_descriptor", StringType(), True),

            # Configuration
            StructField("capture_method", StringType(), True),  # automatic, manual
            StructField("confirmation_method", StringType(), True),  # automatic, manual

            # Complex fields as JSON
            StructField("charges", StringType(), True),
            StructField("payment_method_options", StringType(), True),
            StructField("shipping", StringType(), True),
            StructField("metadata", StringType(), True),

            # Latest charge
            StructField("latest_charge", StringType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": False,
            "object_type": "payment_intent"
        }

        return schema, metadata

    def _get_subscriptions_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the subscriptions table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Status and dates
            StructField("status", StringType(), True),  # active, past_due, unpaid, canceled, etc.
            StructField("current_period_start", LongType(), True),
            StructField("current_period_end", LongType(), True),
            StructField("cancel_at", LongType(), True),
            StructField("canceled_at", LongType(), True),
            StructField("ended_at", LongType(), True),
            StructField("trial_start", LongType(), True),
            StructField("trial_end", LongType(), True),

            # Related objects
            StructField("customer", StringType(), True),
            StructField("default_payment_method", StringType(), True),
            StructField("latest_invoice", StringType(), True),

            # Billing
            StructField("billing_cycle_anchor", LongType(), True),
            StructField("collection_method", StringType(), True),  # charge_automatically, send_invoice
            StructField("days_until_due", LongType(), True),

            # Items (as JSON string)
            StructField("items", StringType(), True),

            # Metadata
            StructField("metadata", StringType(), True),

            # Discount
            StructField("discount", StringType(), True),

            # Cancellation details
            StructField("cancel_at_period_end", BooleanType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": False,
            "object_type": "subscription"
        }

        return schema, metadata

    def _get_invoices_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the invoices table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Status
            StructField("status", StringType(), True),  # draft, open, paid, void, uncollectible
            StructField("paid", BooleanType(), True),

            # Amount fields
            StructField("amount_due", LongType(), True),
            StructField("amount_paid", LongType(), True),
            StructField("amount_remaining", LongType(), True),
            StructField("total", LongType(), True),
            StructField("subtotal", LongType(), True),
            StructField("tax", LongType(), True),
            StructField("currency", StringType(), True),

            # Related objects
            StructField("customer", StringType(), True),
            StructField("subscription", StringType(), True),
            StructField("charge", StringType(), True),
            StructField("payment_intent", StringType(), True),

            # Billing details
            StructField("billing_reason", StringType(), True),
            StructField("collection_method", StringType(), True),
            StructField("customer_email", StringType(), True),
            StructField("customer_name", StringType(), True),

            # Dates
            StructField("due_date", LongType(), True),
            StructField("period_start", LongType(), True),
            StructField("period_end", LongType(), True),

            # Invoice details
            StructField("number", StringType(), True),
            StructField("hosted_invoice_url", StringType(), True),
            StructField("invoice_pdf", StringType(), True),

            # Lines (as JSON string)
            StructField("lines", StringType(), True),

            # Metadata
            StructField("metadata", StringType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": False,
            "object_type": "invoice"
        }

        return schema, metadata

    def _get_products_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the products table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("updated", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Product details
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("active", BooleanType(), True),

            # Categorization
            StructField("type", StringType(), True),  # service, good
            StructField("unit_label", StringType(), True),
            StructField("url", StringType(), True),

            # Images
            StructField("images", ArrayType(StringType()), True),

            # Metadata
            StructField("metadata", StringType(), True),

            # Additional info
            StructField("statement_descriptor", StringType(), True),
            StructField("tax_code", StringType(), True),

            # Shipping
            StructField("shippable", BooleanType(), True),

            # Deletion tracking
            StructField("deleted", BooleanType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": True,
            "object_type": "product"
        }

        return schema, metadata

    def _get_prices_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the prices table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Price details
            StructField("active", BooleanType(), True),
            StructField("currency", StringType(), True),
            StructField("unit_amount", LongType(), True),  # In cents
            StructField("unit_amount_decimal", StringType(), True),

            # Product reference
            StructField("product", StringType(), True),

            # Billing
            StructField("billing_scheme", StringType(), True),  # per_unit, tiered
            StructField("type", StringType(), True),  # one_time, recurring

            # Recurring details (as JSON string for complex structure)
            StructField("recurring", StringType(), True),

            # Lookup key
            StructField("lookup_key", StringType(), True),
            StructField("nickname", StringType(), True),

            # Tax behavior
            StructField("tax_behavior", StringType(), True),

            # Tiers (as JSON string)
            StructField("tiers", StringType(), True),
            StructField("tiers_mode", StringType(), True),

            # Metadata
            StructField("metadata", StringType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": False,
            "object_type": "price"
        }

        return schema, metadata

    def _get_refunds_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the refunds table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Amount fields
            StructField("amount", LongType(), True),  # In cents
            StructField("currency", StringType(), True),

            # Related objects
            StructField("charge", StringType(), True),
            StructField("payment_intent", StringType(), True),

            # Status and reason
            StructField("status", StringType(), True),  # succeeded, pending, failed, canceled
            StructField("reason", StringType(), True),  # duplicate, fraudulent, requested_by_customer

            # Receipt information
            StructField("receipt_number", StringType(), True),

            # Metadata
            StructField("metadata", StringType(), True),

            # Failure information
            StructField("failure_reason", StringType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": False,
            "object_type": "refund"
        }

        return schema, metadata

    def _get_disputes_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the disputes table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Amount fields
            StructField("amount", LongType(), True),  # In cents
            StructField("currency", StringType(), True),

            # Related objects
            StructField("charge", StringType(), True),
            StructField("payment_intent", StringType(), True),

            # Status and reason
            StructField("status", StringType(), True),  # warning_needs_response, warning_under_review, etc.
            StructField("reason", StringType(), True),  # fraudulent, duplicate, etc.

            # Evidence (as JSON string)
            StructField("evidence", StringType(), True),
            StructField("evidence_details", StringType(), True),

            # Refund information
            StructField("is_charge_refundable", BooleanType(), True),

            # Metadata
            StructField("metadata", StringType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": False,
            "object_type": "dispute"
        }

        return schema, metadata

    def _get_payment_methods_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the payment_methods table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Payment method type
            StructField("type", StringType(), True),  # card, bank_account, etc.

            # Customer
            StructField("customer", StringType(), True),

            # Billing details (as JSON string)
            StructField("billing_details", StringType(), True),

            # Card details (as JSON string)
            StructField("card", StringType(), True),

            # Bank account details (as JSON string)
            StructField("us_bank_account", StringType(), True),

            # Metadata
            StructField("metadata", StringType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": False,
            "object_type": "payment_method"
        }

        return schema, metadata

    def _get_balance_transactions_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the balance_transactions table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Amount fields
            StructField("amount", LongType(), True),  # In cents
            StructField("currency", StringType(), True),
            StructField("net", LongType(), True),  # Net amount after fees

            # Fee information
            StructField("fee", LongType(), True),
            StructField("fee_details", StringType(), True),  # JSON string

            # Transaction details
            StructField("type", StringType(), True),  # charge, refund, adjustment, etc.
            StructField("source", StringType(), True),  # ID of related object
            StructField("status", StringType(), True),  # available, pending

            # Description
            StructField("description", StringType(), True),

            # Availability
            StructField("available_on", LongType(), True),  # Unix timestamp

            # Exchange rate (for currency conversion)
            StructField("exchange_rate", StringType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": False,
            "object_type": "balance_transaction"
        }

        return schema, metadata

    def _get_payouts_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the payouts table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Amount fields
            StructField("amount", LongType(), True),  # In cents
            StructField("currency", StringType(), True),

            # Dates
            StructField("arrival_date", LongType(), True),  # Unix timestamp

            # Status and type
            StructField("status", StringType(), True),  # paid, pending, in_transit, canceled, failed
            StructField("type", StringType(), True),  # bank_account, card
            StructField("method", StringType(), True),  # standard, instant

            # Destination
            StructField("destination", StringType(), True),  # Bank account ID

            # Description
            StructField("description", StringType(), True),

            # Balance transaction
            StructField("balance_transaction", StringType(), True),

            # Failure information
            StructField("failure_code", StringType(), True),
            StructField("failure_message", StringType(), True),

            # Metadata
            StructField("metadata", StringType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": False,
            "object_type": "payout"
        }

        return schema, metadata

    def _get_subscription_items_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the subscription_items table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),

            # Related objects
            StructField("subscription", StringType(), True),
            StructField("price", StringType(), True),

            # Quantity
            StructField("quantity", LongType(), True),

            # Billing thresholds (as JSON string)
            StructField("billing_thresholds", StringType(), True),

            # Tax rates
            StructField("tax_rates", StringType(), True),  # JSON array

            # Metadata
            StructField("metadata", StringType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": False,
            "object_type": "subscription_item"
        }

        return schema, metadata

    def _get_invoice_items_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the invoice_items table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Amount fields
            StructField("amount", LongType(), True),  # In cents
            StructField("currency", StringType(), True),

            # Related objects
            StructField("customer", StringType(), True),
            StructField("invoice", StringType(), True),
            StructField("subscription", StringType(), True),
            StructField("price", StringType(), True),

            # Description
            StructField("description", StringType(), True),

            # Quantity and unit amount
            StructField("quantity", LongType(), True),
            StructField("unit_amount", LongType(), True),
            StructField("unit_amount_decimal", StringType(), True),

            # Period
            StructField("period_start", LongType(), True),
            StructField("period_end", LongType(), True),

            # Discounts
            StructField("discounts", StringType(), True),  # JSON array

            # Metadata
            StructField("metadata", StringType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": False,
            "object_type": "invoiceitem"
        }

        return schema, metadata

    def _get_plans_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the plans table (legacy pricing)."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Active status
            StructField("active", BooleanType(), True),

            # Amount fields
            StructField("amount", LongType(), True),  # In cents
            StructField("currency", StringType(), True),

            # Billing interval
            StructField("interval", StringType(), True),  # day, week, month, year
            StructField("interval_count", LongType(), True),

            # Product
            StructField("product", StringType(), True),

            # Nickname
            StructField("nickname", StringType(), True),

            # Usage type
            StructField("usage_type", StringType(), True),  # licensed, metered

            # Aggregate usage (for metered plans)
            StructField("aggregate_usage", StringType(), True),

            # Trial period days
            StructField("trial_period_days", LongType(), True),

            # Tiers (as JSON string)
            StructField("tiers", StringType(), True),
            StructField("tiers_mode", StringType(), True),

            # Metadata
            StructField("metadata", StringType(), True),

            # Deletion tracking
            StructField("deleted", BooleanType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": True,
            "object_type": "plan"
        }

        return schema, metadata

    def _get_events_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the events table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Event type
            StructField("type", StringType(), True),  # e.g., customer.created, charge.succeeded

            # Data (as JSON string containing the affected object)
            StructField("data", StringType(), True),

            # API version
            StructField("api_version", StringType(), True),

            # Request information (as JSON string)
            StructField("request", StringType(), True),

            # Webhooks
            StructField("pending_webhooks", LongType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": False,
            "object_type": "event"
        }

        return schema, metadata

    def _get_coupons_schema(self) -> Tuple[StructType, Dict]:
        """Define schema for the coupons table."""
        schema = StructType([
            # Core fields
            StructField("id", StringType(), False),
            StructField("object", StringType(), True),
            StructField("created", LongType(), True),
            StructField("livemode", BooleanType(), True),

            # Coupon details
            StructField("name", StringType(), True),

            # Discount amount
            StructField("amount_off", LongType(), True),  # In cents (mutually exclusive with percent_off)
            StructField("percent_off", StringType(), True),  # Decimal percentage
            StructField("currency", StringType(), True),  # Required if amount_off is used

            # Duration
            StructField("duration", StringType(), True),  # forever, once, repeating
            StructField("duration_in_months", LongType(), True),

            # Redemption limits
            StructField("max_redemptions", LongType(), True),
            StructField("times_redeemed", LongType(), True),
            StructField("redeem_by", LongType(), True),  # Unix timestamp

            # Validity
            StructField("valid", BooleanType(), True),

            # Applies to (as JSON string)
            StructField("applies_to", StringType(), True),

            # Metadata
            StructField("metadata", StringType(), True),
        ])

        metadata = {
            "primary_key": "id",
            "cursor_field": "created",
            "ingestion_type": "cdc",
            "supports_incremental": True,
            "supports_deleted": False,
            "object_type": "coupon"
        }

        return schema, metadata

    def _read_data_full(self, table_name: str) -> Tuple[List[Dict], Dict]:
        """
        Read all data from a Stripe table (full refresh).

        Args:
            table_name: Name of the table

        Returns:
            Tuple of (all_records, offset)
        """
        config = self._object_config[table_name]
        endpoint = config["endpoint"]
        cursor_field = config["cursor_field"]

        all_records = []
        starting_after = None
        latest_cursor_value = 0

        while True:
            # Build request parameters
            params = {
                "limit": 100  # Max allowed by Stripe
            }

            if starting_after:
                params["starting_after"] = starting_after

            # Make API request
            url = f"{self.base_url}/{endpoint}"
            response = requests.get(url, auth=self.auth, params=params)

            if response.status_code != 200:
                raise Exception(f"Stripe API error for {table_name}: {response.status_code} {response.text}")

            data = response.json()
            records = data.get("data", [])

            if not records:
                break

            # Transform records
            transformed_records = self._transform_records(records, table_name)
            all_records.extend(transformed_records)

            # Track the latest cursor value for checkpointing
            for record in records:
                cursor_value = record.get(cursor_field, 0)
                if cursor_value > latest_cursor_value:
                    latest_cursor_value = cursor_value

            # Check if there are more pages
            has_more = data.get("has_more", False)
            if not has_more:
                break

            # Get the last object ID for pagination
            starting_after = records[-1]["id"]

            # Rate limiting - be nice to the API
            time.sleep(0.1)

        # Return records and offset for next incremental sync
        offset = {cursor_field: latest_cursor_value} if latest_cursor_value > 0 else {}
        return all_records, offset

    def _read_data_incremental(self, table_name: str, start_offset: dict) -> Tuple[List[Dict], Dict]:
        """
        Read incremental data from a Stripe table using cursor.

        Args:
            table_name: Name of the table
            start_offset: Dictionary with cursor field value

        Returns:
            Tuple of (new_records, new_offset)
        """
        config = self._object_config[table_name]
        endpoint = config["endpoint"]
        cursor_field = config["cursor_field"]

        # Get the starting point from offset
        cursor_start = start_offset.get(cursor_field, 0)

        all_records = []
        starting_after = None
        latest_cursor_value = cursor_start

        while True:
            # Build request parameters for incremental fetch
            params = {
                "limit": 100,
                f"{cursor_field}[gte]": cursor_start  # Greater than or equal to last cursor
            }

            if starting_after:
                params["starting_after"] = starting_after

            # Make API request
            url = f"{self.base_url}/{endpoint}"
            response = requests.get(url, auth=self.auth, params=params)

            if response.status_code != 200:
                raise Exception(f"Stripe API error for {table_name}: {response.status_code} {response.text}")

            data = response.json()
            records = data.get("data", [])

            if not records:
                break

            # Transform records
            transformed_records = self._transform_records(records, table_name)
            all_records.extend(transformed_records)

            # Track the latest cursor value
            for record in records:
                cursor_value = record.get(cursor_field, 0)
                if cursor_value > latest_cursor_value:
                    latest_cursor_value = cursor_value

            # Check if there are more pages
            has_more = data.get("has_more", False)
            if not has_more:
                break

            # Get the last object ID for pagination
            starting_after = records[-1]["id"]

            # Rate limiting
            time.sleep(0.1)

        # Return new offset for next sync
        offset = {cursor_field: latest_cursor_value}
        return all_records, offset

    def _transform_records(self, records: List[Dict], table_name: str) -> List[Dict]:
        """
        Transform Stripe API records to match schema.

        Args:
            records: Raw records from Stripe API
            table_name: Name of the table

        Returns:
            List of transformed records
        """
        if table_name == "customers":
            return [self._transform_customer_record(record) for record in records]
        elif table_name == "charges":
            return [self._transform_charge_record(record) for record in records]
        elif table_name == "payment_intents":
            return [self._transform_payment_intent_record(record) for record in records]
        elif table_name == "subscriptions":
            return [self._transform_subscription_record(record) for record in records]
        elif table_name == "invoices":
            return [self._transform_invoice_record(record) for record in records]
        elif table_name == "products":
            return [self._transform_product_record(record) for record in records]
        elif table_name == "prices":
            return [self._transform_price_record(record) for record in records]
        elif table_name == "refunds":
            return [self._transform_refund_record(record) for record in records]
        elif table_name == "disputes":
            return [self._transform_dispute_record(record) for record in records]
        elif table_name == "payment_methods":
            return [self._transform_payment_method_record(record) for record in records]
        elif table_name == "balance_transactions":
            return [self._transform_balance_transaction_record(record) for record in records]
        elif table_name == "payouts":
            return [self._transform_payout_record(record) for record in records]
        elif table_name == "invoice_items":
            return [self._transform_invoice_item_record(record) for record in records]
        elif table_name == "plans":
            return [self._transform_plan_record(record) for record in records]
        elif table_name == "events":
            return [self._transform_event_record(record) for record in records]
        elif table_name == "coupons":
            return [self._transform_coupon_record(record) for record in records]
        else:
            return records

    def _transform_customer_record(self, record: Dict) -> Dict:
        """
        Transform a Stripe Customer object to match our schema.

        Args:
            record: Raw customer record from Stripe API

        Returns:
            Transformed customer record
        """
        # Extract address fields
        address = record.get("address") or {}

        # Extract shipping fields
        shipping = record.get("shipping") or {}
        shipping_address = shipping.get("address") or {}

        # Extract invoice settings
        invoice_settings = record.get("invoice_settings")
        invoice_settings_json = json.dumps(invoice_settings) if invoice_settings else None

        # Extract metadata
        metadata = record.get("metadata")
        metadata_json = json.dumps(metadata) if metadata else None

        # Extract discount
        discount = record.get("discount")
        discount_json = json.dumps(discount) if discount else None

        transformed = {
            # Core fields
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "updated": record.get("updated"),  # May not exist for customers
            "livemode": record.get("livemode"),

            # Customer details
            "email": record.get("email"),
            "name": record.get("name"),
            "phone": record.get("phone"),
            "description": record.get("description"),

            # Address (flattened)
            "address_line1": address.get("line1"),
            "address_line2": address.get("line2"),
            "address_city": address.get("city"),
            "address_state": address.get("state"),
            "address_postal_code": address.get("postal_code"),
            "address_country": address.get("country"),

            # Shipping (flattened)
            "shipping_name": shipping.get("name"),
            "shipping_phone": shipping.get("phone"),
            "shipping_address_line1": shipping_address.get("line1"),
            "shipping_address_line2": shipping_address.get("line2"),
            "shipping_address_city": shipping_address.get("city"),
            "shipping_address_state": shipping_address.get("state"),
            "shipping_address_postal_code": shipping_address.get("postal_code"),
            "shipping_address_country": shipping_address.get("country"),

            # Financial fields
            "balance": record.get("balance"),
            "currency": record.get("currency"),
            "delinquent": record.get("delinquent"),

            # Preferences
            "preferred_locales": record.get("preferred_locales"),

            # Complex fields as JSON strings
            "invoice_settings": invoice_settings_json,
            "metadata": metadata_json,
            "discount": discount_json,

            # Tax information
            "tax_exempt": record.get("tax_exempt"),

            # References
            "default_source": record.get("default_source"),
            "invoice_prefix": record.get("invoice_prefix"),

            # Deletion tracking
            "deleted": record.get("deleted", False)
        }

        return transformed

    def _transform_charge_record(self, record: Dict) -> Dict:
        """Transform a Stripe Charge object to match our schema."""
        # Convert complex fields to JSON strings
        billing_details = record.get("billing_details")
        payment_method_details = record.get("payment_method_details")
        outcome = record.get("outcome")
        metadata = record.get("metadata")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "livemode": record.get("livemode"),
            "amount": record.get("amount"),
            "amount_captured": record.get("amount_captured"),
            "amount_refunded": record.get("amount_refunded"),
            "currency": record.get("currency"),
            "status": record.get("status"),
            "paid": record.get("paid"),
            "refunded": record.get("refunded"),
            "captured": record.get("captured"),
            "disputed": record.get("disputed"),
            "customer": record.get("customer"),
            "invoice": record.get("invoice"),
            "payment_intent": record.get("payment_intent"),
            "payment_method": record.get("payment_method"),
            "description": record.get("description"),
            "receipt_email": record.get("receipt_email"),
            "receipt_url": record.get("receipt_url"),
            "statement_descriptor": record.get("statement_descriptor"),
            "billing_details": json.dumps(billing_details) if billing_details else None,
            "payment_method_details": json.dumps(payment_method_details) if payment_method_details else None,
            "outcome": json.dumps(outcome) if outcome else None,
            "metadata": json.dumps(metadata) if metadata else None,
            "failure_code": record.get("failure_code"),
            "failure_message": record.get("failure_message"),
        }

    def _transform_payment_intent_record(self, record: Dict) -> Dict:
        """Transform a Stripe PaymentIntent object to match our schema."""
        charges = record.get("charges")
        payment_method_options = record.get("payment_method_options")
        shipping = record.get("shipping")
        metadata = record.get("metadata")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "livemode": record.get("livemode"),
            "amount": record.get("amount"),
            "amount_capturable": record.get("amount_capturable"),
            "amount_received": record.get("amount_received"),
            "currency": record.get("currency"),
            "status": record.get("status"),
            "canceled_at": record.get("canceled_at"),
            "cancellation_reason": record.get("cancellation_reason"),
            "customer": record.get("customer"),
            "invoice": record.get("invoice"),
            "payment_method": record.get("payment_method"),
            "description": record.get("description"),
            "receipt_email": record.get("receipt_email"),
            "statement_descriptor": record.get("statement_descriptor"),
            "capture_method": record.get("capture_method"),
            "confirmation_method": record.get("confirmation_method"),
            "charges": json.dumps(charges) if charges else None,
            "payment_method_options": json.dumps(payment_method_options) if payment_method_options else None,
            "shipping": json.dumps(shipping) if shipping else None,
            "metadata": json.dumps(metadata) if metadata else None,
            "latest_charge": record.get("latest_charge"),
        }

    def _transform_subscription_record(self, record: Dict) -> Dict:
        """Transform a Stripe Subscription object to match our schema."""
        items = record.get("items")
        metadata = record.get("metadata")
        discount = record.get("discount")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "livemode": record.get("livemode"),
            "status": record.get("status"),
            "current_period_start": record.get("current_period_start"),
            "current_period_end": record.get("current_period_end"),
            "cancel_at": record.get("cancel_at"),
            "canceled_at": record.get("canceled_at"),
            "ended_at": record.get("ended_at"),
            "trial_start": record.get("trial_start"),
            "trial_end": record.get("trial_end"),
            "customer": record.get("customer"),
            "default_payment_method": record.get("default_payment_method"),
            "latest_invoice": record.get("latest_invoice"),
            "billing_cycle_anchor": record.get("billing_cycle_anchor"),
            "collection_method": record.get("collection_method"),
            "days_until_due": record.get("days_until_due"),
            "items": json.dumps(items) if items else None,
            "metadata": json.dumps(metadata) if metadata else None,
            "discount": json.dumps(discount) if discount else None,
            "cancel_at_period_end": record.get("cancel_at_period_end"),
        }

    def _transform_invoice_record(self, record: Dict) -> Dict:
        """Transform a Stripe Invoice object to match our schema."""
        lines = record.get("lines")
        metadata = record.get("metadata")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "livemode": record.get("livemode"),
            "status": record.get("status"),
            "paid": record.get("paid"),
            "amount_due": record.get("amount_due"),
            "amount_paid": record.get("amount_paid"),
            "amount_remaining": record.get("amount_remaining"),
            "total": record.get("total"),
            "subtotal": record.get("subtotal"),
            "tax": record.get("tax"),
            "currency": record.get("currency"),
            "customer": record.get("customer"),
            "subscription": record.get("subscription"),
            "charge": record.get("charge"),
            "payment_intent": record.get("payment_intent"),
            "billing_reason": record.get("billing_reason"),
            "collection_method": record.get("collection_method"),
            "customer_email": record.get("customer_email"),
            "customer_name": record.get("customer_name"),
            "due_date": record.get("due_date"),
            "period_start": record.get("period_start"),
            "period_end": record.get("period_end"),
            "number": record.get("number"),
            "hosted_invoice_url": record.get("hosted_invoice_url"),
            "invoice_pdf": record.get("invoice_pdf"),
            "lines": json.dumps(lines) if lines else None,
            "metadata": json.dumps(metadata) if metadata else None,
        }

    def _transform_product_record(self, record: Dict) -> Dict:
        """Transform a Stripe Product object to match our schema."""
        metadata = record.get("metadata")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "updated": record.get("updated"),
            "livemode": record.get("livemode"),
            "name": record.get("name"),
            "description": record.get("description"),
            "active": record.get("active"),
            "type": record.get("type"),
            "unit_label": record.get("unit_label"),
            "url": record.get("url"),
            "images": record.get("images"),
            "metadata": json.dumps(metadata) if metadata else None,
            "statement_descriptor": record.get("statement_descriptor"),
            "tax_code": record.get("tax_code"),
            "shippable": record.get("shippable"),
            "deleted": record.get("deleted", False),
        }

    def _transform_price_record(self, record: Dict) -> Dict:
        """Transform a Stripe Price object to match our schema."""
        recurring = record.get("recurring")
        tiers = record.get("tiers")
        metadata = record.get("metadata")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "livemode": record.get("livemode"),
            "active": record.get("active"),
            "currency": record.get("currency"),
            "unit_amount": record.get("unit_amount"),
            "unit_amount_decimal": record.get("unit_amount_decimal"),
            "product": record.get("product"),
            "billing_scheme": record.get("billing_scheme"),
            "type": record.get("type"),
            "recurring": json.dumps(recurring) if recurring else None,
            "lookup_key": record.get("lookup_key"),
            "nickname": record.get("nickname"),
            "tax_behavior": record.get("tax_behavior"),
            "tiers": json.dumps(tiers) if tiers else None,
            "tiers_mode": record.get("tiers_mode"),
            "metadata": json.dumps(metadata) if metadata else None,
        }

    def _transform_refund_record(self, record: Dict) -> Dict:
        """Transform a Stripe Refund object to match our schema."""
        metadata = record.get("metadata")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "livemode": record.get("livemode"),
            "amount": record.get("amount"),
            "currency": record.get("currency"),
            "charge": record.get("charge"),
            "payment_intent": record.get("payment_intent"),
            "status": record.get("status"),
            "reason": record.get("reason"),
            "receipt_number": record.get("receipt_number"),
            "metadata": json.dumps(metadata) if metadata else None,
            "failure_reason": record.get("failure_reason"),
        }

    def _transform_dispute_record(self, record: Dict) -> Dict:
        """Transform a Stripe Dispute object to match our schema."""
        evidence = record.get("evidence")
        evidence_details = record.get("evidence_details")
        metadata = record.get("metadata")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "livemode": record.get("livemode"),
            "amount": record.get("amount"),
            "currency": record.get("currency"),
            "charge": record.get("charge"),
            "payment_intent": record.get("payment_intent"),
            "status": record.get("status"),
            "reason": record.get("reason"),
            "evidence": json.dumps(evidence) if evidence else None,
            "evidence_details": json.dumps(evidence_details) if evidence_details else None,
            "is_charge_refundable": record.get("is_charge_refundable"),
            "metadata": json.dumps(metadata) if metadata else None,
        }

    def _transform_payment_method_record(self, record: Dict) -> Dict:
        """Transform a Stripe PaymentMethod object to match our schema."""
        billing_details = record.get("billing_details")
        card = record.get("card")
        us_bank_account = record.get("us_bank_account")
        metadata = record.get("metadata")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "livemode": record.get("livemode"),
            "type": record.get("type"),
            "customer": record.get("customer"),
            "billing_details": json.dumps(billing_details) if billing_details else None,
            "card": json.dumps(card) if card else None,
            "us_bank_account": json.dumps(us_bank_account) if us_bank_account else None,
            "metadata": json.dumps(metadata) if metadata else None,
        }

    def _transform_balance_transaction_record(self, record: Dict) -> Dict:
        """Transform a Stripe BalanceTransaction object to match our schema."""
        fee_details = record.get("fee_details")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "livemode": record.get("livemode"),
            "amount": record.get("amount"),
            "currency": record.get("currency"),
            "net": record.get("net"),
            "fee": record.get("fee"),
            "fee_details": json.dumps(fee_details) if fee_details else None,
            "type": record.get("type"),
            "source": record.get("source"),
            "status": record.get("status"),
            "description": record.get("description"),
            "available_on": record.get("available_on"),
            "exchange_rate": str(record.get("exchange_rate")) if record.get("exchange_rate") else None,
        }

    def _transform_payout_record(self, record: Dict) -> Dict:
        """Transform a Stripe Payout object to match our schema."""
        metadata = record.get("metadata")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "livemode": record.get("livemode"),
            "amount": record.get("amount"),
            "currency": record.get("currency"),
            "arrival_date": record.get("arrival_date"),
            "status": record.get("status"),
            "type": record.get("type"),
            "method": record.get("method"),
            "destination": record.get("destination"),
            "description": record.get("description"),
            "balance_transaction": record.get("balance_transaction"),
            "failure_code": record.get("failure_code"),
            "failure_message": record.get("failure_message"),
            "metadata": json.dumps(metadata) if metadata else None,
        }

    def _transform_subscription_item_record(self, record: Dict) -> Dict:
        """Transform a Stripe SubscriptionItem object to match our schema."""
        billing_thresholds = record.get("billing_thresholds")
        tax_rates = record.get("tax_rates")
        metadata = record.get("metadata")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "subscription": record.get("subscription"),
            "price": record.get("price"),
            "quantity": record.get("quantity"),
            "billing_thresholds": json.dumps(billing_thresholds) if billing_thresholds else None,
            "tax_rates": json.dumps(tax_rates) if tax_rates else None,
            "metadata": json.dumps(metadata) if metadata else None,
        }

    def _transform_invoice_item_record(self, record: Dict) -> Dict:
        """Transform a Stripe InvoiceItem object to match our schema."""
        discounts = record.get("discounts")
        metadata = record.get("metadata")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "livemode": record.get("livemode"),
            "amount": record.get("amount"),
            "currency": record.get("currency"),
            "customer": record.get("customer"),
            "invoice": record.get("invoice"),
            "subscription": record.get("subscription"),
            "price": record.get("price"),
            "description": record.get("description"),
            "quantity": record.get("quantity"),
            "unit_amount": record.get("unit_amount"),
            "unit_amount_decimal": record.get("unit_amount_decimal"),
            "period_start": record.get("period", {}).get("start") if record.get("period") else None,
            "period_end": record.get("period", {}).get("end") if record.get("period") else None,
            "discounts": json.dumps(discounts) if discounts else None,
            "metadata": json.dumps(metadata) if metadata else None,
        }

    def _transform_plan_record(self, record: Dict) -> Dict:
        """Transform a Stripe Plan object to match our schema."""
        tiers = record.get("tiers")
        metadata = record.get("metadata")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "livemode": record.get("livemode"),
            "active": record.get("active"),
            "amount": record.get("amount"),
            "currency": record.get("currency"),
            "interval": record.get("interval"),
            "interval_count": record.get("interval_count"),
            "product": record.get("product"),
            "nickname": record.get("nickname"),
            "usage_type": record.get("usage_type"),
            "aggregate_usage": record.get("aggregate_usage"),
            "trial_period_days": record.get("trial_period_days"),
            "tiers": json.dumps(tiers) if tiers else None,
            "tiers_mode": record.get("tiers_mode"),
            "metadata": json.dumps(metadata) if metadata else None,
            "deleted": record.get("deleted", False),
        }

    def _transform_event_record(self, record: Dict) -> Dict:
        """Transform a Stripe Event object to match our schema."""
        data = record.get("data")
        request = record.get("request")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "livemode": record.get("livemode"),
            "type": record.get("type"),
            "data": json.dumps(data) if data else None,
            "api_version": record.get("api_version"),
            "request": json.dumps(request) if request else None,
            "pending_webhooks": record.get("pending_webhooks"),
        }

    def _transform_coupon_record(self, record: Dict) -> Dict:
        """Transform a Stripe Coupon object to match our schema."""
        applies_to = record.get("applies_to")
        metadata = record.get("metadata")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created": record.get("created"),
            "livemode": record.get("livemode"),
            "name": record.get("name"),
            "amount_off": record.get("amount_off"),
            "percent_off": str(record.get("percent_off")) if record.get("percent_off") else None,
            "currency": record.get("currency"),
            "duration": record.get("duration"),
            "duration_in_months": record.get("duration_in_months"),
            "max_redemptions": record.get("max_redemptions"),
            "times_redeemed": record.get("times_redeemed"),
            "redeem_by": record.get("redeem_by"),
            "valid": record.get("valid"),
            "applies_to": json.dumps(applies_to) if applies_to else None,
            "metadata": json.dumps(metadata) if metadata else None,
        }

    def test_connection(self) -> dict:
        """
        Test the connection to Stripe API.

        Returns:
            Dictionary with status and message
        """
        try:
            url = f"{self.base_url}/customers?limit=1"
            response = requests.get(url, auth=self.auth)

            if response.status_code == 200:
                return {"status": "success", "message": "Connection successful"}
            else:
                return {
                    "status": "error",
                    "message": f"API error: {response.status_code} {response.text}"
                }
        except Exception as e:
            return {"status": "error", "message": f"Connection failed: {str(e)}"}
