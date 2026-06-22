"""Configuration-driven data quality rules."""

ORDERS_DQ_RULES: list[dict] = [
    {
        "rule_id": "orders_order_id_not_null",
        "check_type": "not_null",
        "column": "order_id",
        "severity": "critical",
        "params": {},
    },
    {
        "rule_id": "orders_order_id_unique",
        "check_type": "unique",
        "column": "order_id",
        "severity": "critical",
        "params": {},
    },
    {
        "rule_id": "orders_customer_id_not_null",
        "check_type": "not_null",
        "column": "customer_id",
        "severity": "critical",
        "params": {},
    },
    {
        "rule_id": "orders_customer_referential",
        "check_type": "referential",
        "column": "customer_id",
        "severity": "critical",
        "params": {
            "reference_table": "globalmart.bronze.customers",
            "reference_column": "customer_id",
        },
    },
    {
        "rule_id": "orders_status_accepted",
        "check_type": "accepted_values",
        "column": "order_status",
        "severity": "warn",
        "params": {
            "values": [
                "delivered",
                "shipped",
                "processing",
                "invoiced",
                "created",
                "approved",
                "unavailable",
                "canceled",
            ],
        },
    },
    {
        "rule_id": "orders_purchase_timestamp_not_null",
        "check_type": "not_null",
        "column": "order_purchase_timestamp",
        "severity": "critical",
        "params": {},
    },
]
