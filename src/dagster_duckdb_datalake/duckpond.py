"""Core abstractions for composing and executing DuckDB SQL plans.

What this file is:
    The project's core data-layer module with SQL plan objects, SQL rendering,
    DuckDB execution, and Dagster I/O manager logic.

Why it is relevant:
    It contains the main input -> transformation -> output mechanics:
    asset SQL plan -> rendered DuckDB SQL -> Parquet files on S3.

This module separates three concerns:
1) expressing query intent (`SQL` objects),
2) rendering nested SQL/bindings into executable SQL text,
3) executing rendered SQL in an ephemeral DuckDB session.
"""

from dataclasses import dataclass, field
from string import Template
from typing import Any

import pandas as pd
from dagster import ConfigurableIOManager, InputContext, OutputContext
from duckdb import connect
from sqlescapy import sqlescape


@dataclass(init=False)
class SQL:
    """A lightweight SQL template plus placeholder bindings.

    Think of this as a "query recipe":
    - `sql` is the recipe text with placeholders like `$name`.
    - `bindings` are the real values for those placeholders.

    Example input:
        SQL(
            "select * from $df where id > $min_id",
            df=my_dataframe,
            min_id=10,
        )

    Example stored state (conceptually):
        sql = "select * from $df where id > $min_id"
        bindings = {"df": my_dataframe, "min_id": 10}

    Placeholders use Python `string.Template` style (e.g. `$name`). Values can be
    primitives, pandas DataFrames, or nested `SQL` objects.
    """

    sql: str
    bindings: dict[str, Any] = field(default_factory=dict)

    def __init__(self, sql: str, **bindings: Any):
        """Create a SQL plan with named binding values."""
        self.sql = sql
        self.bindings = bindings


def sql_to_string(statement: SQL) -> str:
    """Render a `SQL` plan into executable SQL text.

    Rendering rules:
    - DataFrames become temporary relation names (`df_<id>`).
    - Nested `SQL` objects are rendered recursively and wrapped in parentheses.
    - Strings are SQL-escaped and quoted.
    - Numeric/boolean values are inlined directly.
    - `None` becomes `null`.

    Simple example 1:
        input:
            SQL("select * from users where country = $country", country="TR")
        output:
            "select * from users where country = 'TR'"

    Simple example 2 (with nested SQL):
        input:
            base = SQL("select * from $df", df=df)
            outer = SQL("select count(*) as c from $base", base=base)
        output pattern:
            "select count(*) as c from (select * from df_<some_id>)"

    Why this function exists:
        DuckDB executes SQL text. Our assets build structured `SQL` objects, so we
        need one place that converts those objects into final runnable SQL.
    """

    replacements: dict[str, str] = {}
    for key, value in statement.bindings.items():
        if isinstance(value, pd.DataFrame):
            replacements[key] = f"df_{id(value)}"
        elif isinstance(value, SQL):
            replacements[key] = f"({sql_to_string(value)})"
        elif isinstance(value, str):
            replacements[key] = f"'{sqlescape(value)}'"
        elif isinstance(value, (int, float, bool)):
            replacements[key] = str(value)
        elif value is None:
            replacements[key] = "null"
        else:
            raise ValueError(f"Invalid type for {key}: {type(value).__name__}")
    return Template(statement.sql).safe_substitute(replacements)


def collect_dataframes(statement: SQL) -> dict[str, pd.DataFrame]:
    """Collect all DataFrames referenced by a (possibly nested) `SQL` plan.

    The returned mapping uses the same generated names expected by
    `sql_to_string`, allowing these DataFrames to be registered with DuckDB
    before query execution.

    Example:
        input SQL tree:
            outer -> binds `orders_sql`
            orders_sql -> binds df_orders

        output:
            {
                "df_<id_of_df_orders>": df_orders
            }

    Why this function exists:
        If SQL references DataFrames, DuckDB must know those DataFrames as temporary
        tables first. This function finds all of them.
    """

    dataframes: dict[str, pd.DataFrame] = {}
    for value in statement.bindings.values():
        if isinstance(value, pd.DataFrame):
            dataframes[f"df_{id(value)}"] = value
        elif isinstance(value, SQL):
            dataframes.update(collect_dataframes(value))
    return dataframes


class DuckDB:
    """Small execution wrapper around DuckDB for `SQL` plans."""

    def __init__(self, options: str = ""):
        """Store optional startup SQL (e.g., S3 credentials/settings)."""
        self.options = options

    def query(self, statement: SQL):
        """Execute a `SQL` plan and return a pandas DataFrame result.

        Execution flow:
        1) open an in-memory DuckDB connection,
        2) try to load `httpfs` (required for HTTP/S3 access),
        3) apply optional startup SQL,
        4) register all bound DataFrames as temporary relations,
        5) render and execute the SQL text.

        Example input:
            df = pandas.DataFrame({"n": [1, 2, 3]})
            statement = SQL("select sum(n) as total from $df", df=df)

        Example output:
            pandas DataFrame with one row:
                total
                6

        In simple words:
            this is the "runner". It takes your SQL recipe, prepares DuckDB, runs
            it, and gives you a DataFrame back.
        """

        db = connect(":memory:")

        try:
            db.query("load httpfs;")
        except Exception:
            try:
                db.query("install httpfs; load httpfs;")
            except Exception:
                pass

        if self.options:
            db.query(self.options)

        for name, dataframe in collect_dataframes(statement).items():
            db.register(name, dataframe)

        result = db.query(sql_to_string(statement))
        if result is None:
            return None
        return result.df()


class DuckPondIOManager(ConfigurableIOManager):
    """Dagster I/O manager that stores assets as Parquet files on S3.

    In simple words:
        - `handle_output` saves an asset's SQL result into S3 as a Parquet file.
        - `load_input` gives downstream assets a SQL query that reads that file.

    Why this class exists:
        It keeps storage code out of asset functions. Assets only describe
        transformations, while this class handles where data is physically stored.

    Example config (conceptual):
        DuckPondIOManager(
            bucket_name="datalake",
            duckdb_options="set s3_endpoint='localhost:4566'; ...",
            prefix="jaffle",
        )
    """

    bucket_name: str
    duckdb_options: str
    prefix: str = ""

    @property
    def duckdb(self) -> DuckDB:
        """Build a fresh DuckDB runner configured for this I/O manager.

        Input:
            No direct method input; uses `self.duckdb_options`.

        Output:
            A configured `DuckDB` helper instance.
        """

        return DuckDB(self.duckdb_options)

    def _get_s3_url(self, context: InputContext | OutputContext) -> str:
        """Compute the S3 path for an asset input/output as a `.parquet` file.

        Input:
            Dagster context describing either an output being written or an input
            being loaded.

        Transformation:
            asset identifier -> path segments joined with `/` -> S3 URL.

        Output example:
            "s3://datalake/jaffle/stg_orders.parquet"
        """

        if context.has_asset_key:
            identifier = context.get_asset_identifier()
        else:
            identifier = context.get_identifier()

        clean_prefix = self.prefix.strip("/")
        prefix_part = f"{clean_prefix}/" if clean_prefix else ""
        identifier_part = "/".join(identifier)
        return f"s3://{self.bucket_name}/{prefix_part}{identifier_part}.parquet"

    def handle_output(self, context: OutputContext, select_statement: SQL | None) -> None:
        """Persist an asset's SQL result to Parquet in S3.

        Input:
            - `context`: Dagster output context (tells us which asset is writing).
            - `select_statement`: SQL recipe returned by the asset.

        Behavior:
            - If the asset returned `None`, do nothing.
            - If the asset did not return `SQL`, raise a clear error.
            - Otherwise run DuckDB `COPY (...) TO s3://... (format parquet)`.

        Output:
            No Python return value. Side effect is the Parquet object written to S3.
        """

        if select_statement is None:
            return

        if not isinstance(select_statement, SQL):
            raise ValueError(f"Expected asset to return SQL; got {select_statement!r}")

        self.duckdb.query(
            SQL(
                "copy ($select_statement) to $url (format parquet)",
                select_statement=select_statement,
                url=self._get_s3_url(context),
            )
        )

    def load_input(self, context: InputContext) -> SQL:
        """Return a SQL plan that reads an upstream Parquet file from S3.

        Input:
            `context` describing which upstream asset is being loaded.

        Output example:
            SQL("select * from read_parquet($url)", url="s3://datalake/...parquet")

        In simple words:
            downstream assets receive a SQL recipe pointing to the upstream file,
            not a pandas DataFrame loaded eagerly into memory.
        """

        return SQL("select * from read_parquet($url)", url=self._get_s3_url(context))