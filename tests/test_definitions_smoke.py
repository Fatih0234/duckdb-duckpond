"""Smoke test for Dagster definitions module loading.

What this file is:
    A minimal import test for the `defs` object.

Why it is relevant:
    It catches broken wiring early (resource config/import issues) before running
    the full UI and materialization flow.
"""

from dagster_duckdb_datalake.definitions import defs


def test_definitions_loads():
    loaded = defs
    assert loaded is not None