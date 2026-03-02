"""Unit tests for DuckPond core SQL and execution abstractions.

What this file is:
    Focused tests for SQL rendering, DataFrame collection, and DuckDB query runs.

Why it is relevant:
    It verifies the core engine works independently before wiring full Dagster
    pipelines and S3 materialization.
"""

import pandas as pd

from dagster_duckdb_datalake.duckpond import DuckDB, SQL, collect_dataframes, sql_to_string


def test_sql_to_string_handles_nested_sql_and_scalars():
    inner = SQL("select * from $df where value > $min_value", df=pd.DataFrame({"value": [1, 2, 3]}), min_value=1)
    outer = SQL("select count(*) as cnt from $inner", inner=inner)

    rendered = sql_to_string(outer)

    assert rendered.startswith("select count(*) as cnt from (select * from df_")
    assert "where value > 1" in rendered


def test_collect_dataframes_collects_nested_dataframes():
    df = pd.DataFrame({"a": [1, 2]})
    nested = SQL("select * from $df", df=df)
    statement = SQL("select * from $nested", nested=nested)

    dataframes = collect_dataframes(statement)

    assert f"df_{id(df)}" in dataframes
    assert dataframes[f"df_{id(df)}"].equals(df)


def test_duckdb_query_executes_sql_statement():
    df = pd.DataFrame({"n": [1, 2, 3]})
    statement = SQL("select sum(n) as total from $df", df=df)

    result = DuckDB().query(statement)

    assert result is not None
    assert int(result.iloc[0]["total"]) == 6