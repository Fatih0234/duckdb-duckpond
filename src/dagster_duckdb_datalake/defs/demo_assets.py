"""Jaffle-style staging assets for raw source ingestion.

What this file is:
    Staging-layer assets that ingest raw CSV data and normalize column names/types.

Why it is relevant:
    This is the "foundation layer" before marts/gold models. It turns external,
    source-shaped data into consistent, warehouse-friendly tables.

Role in data flow:
    input (raw CSV files) -> transformation (light cleaning/renaming) -> output
    (staged Parquet tables in S3 via DuckPondIOManager).
"""

import pandas as pd
from dagster import asset

from dagster_duckdb_datalake.duckpond import SQL


RAW_CUSTOMERS_URL = "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_customers.csv"
RAW_ORDERS_URL = "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_orders.csv"
RAW_PAYMENTS_URL = "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_payments.csv"


@asset
def stg_customers() -> SQL:
    """Load and normalize raw customers data.

    Purpose:
        Create a stable staging table with warehouse-friendly customer column names.

    Returns:
        SQL: a query plan selecting from a pandas DataFrame (`$customers_df`).

    Example output shape:
        customer_id | first_name | last_name
        ------------|------------|----------
        1           | Michael    | P.
    """

    # Input: raw CSV from the Jaffle Shop seed file.
    customers_df = pd.read_csv(RAW_CUSTOMERS_URL)

    # Transformation: rename source id column to an analytics-friendly key name.
    customers_df.rename(columns={"id": "customer_id"}, inplace=True)

    # Output: a SQL plan that the IO manager will materialize to Parquet.
    return SQL("select * from $customers_df", customers_df=customers_df)


@asset
def stg_orders() -> SQL:
    """Load and normalize raw orders data.

    Purpose:
        Standardize order identifiers and customer keys for downstream joins.

    Returns:
        SQL: query plan selecting from normalized order DataFrame.

    Example output shape:
        order_id | customer_id | order_date | status
        ---------|-------------|------------|--------
        1        | 1           | 2018-01-01 | returned
    """

    # Input: raw CSV from source system extract.
    orders_df = pd.read_csv(RAW_ORDERS_URL)

    # Transformation: normalize IDs to names we will reuse in marts.
    orders_df.rename(columns={"id": "order_id", "user_id": "customer_id"}, inplace=True)

    # Output: SQL plan over normalized orders data.
    return SQL("select * from $orders_df", orders_df=orders_df)


@asset
def stg_payments() -> SQL:
    """Load and normalize raw payments data.

    Purpose:
        Prepare payments in analytics units by converting cents to dollars.

    Returns:
        SQL: query plan selecting from normalized payments DataFrame.

    Example transformation:
        input amount: 1099 (cents)
        output amount: 10.99 (dollars)
    """

    # Input: raw payments CSV.
    payments_df = pd.read_csv(RAW_PAYMENTS_URL)

    # Transformation 1: align primary key naming convention.
    payments_df.rename(columns={"id": "payment_id"}, inplace=True)

    # Transformation 2: convert cents -> dollars to make downstream metrics readable.
    payments_df["amount"] = payments_df["amount"].map(lambda amount: amount / 100)

    # Output: staged SQL plan for downstream marts/gold assets.
    return SQL("select * from $payments_df", payments_df=payments_df)
