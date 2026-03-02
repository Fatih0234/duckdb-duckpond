"""Project-level Dagster definitions wiring.

What this file is:
    The main Dagster definitions entrypoint used by `dg dev`.

Why it is relevant:
    It connects discovered project definitions to infrastructure resources,
    specifically the custom `DuckPondIOManager` that persists assets to S3.
"""

from pathlib import Path

from dagster import definitions, load_from_defs_folder

from dagster_duckdb_datalake.duckpond import DuckPondIOManager


DUCKDB_LOCALSTACK_OPTIONS = """
set s3_region='us-east-1';
set s3_access_key_id='test';
set s3_secret_access_key='test';
set s3_endpoint='localhost:4566';
set s3_use_ssl=false;
set s3_url_style='path';
"""


@definitions
def defs():
    """Load project definitions and attach the default I/O manager.

    In simple words:
        - `load_from_defs_folder(...)` finds assets/jobs/schedules in `defs/`.
        - `resources={"io_manager": ...}` tells Dagster how assets are stored.

    Why LocalStack options are needed:
        DuckDB must know how to connect to your local S3-compatible endpoint
        (`localhost:4566`) and use test credentials expected by LocalStack.
    """

    loaded_defs = load_from_defs_folder(path_within_project=Path(__file__).parent)

    return loaded_defs.with_resources(
        {
            "io_manager": DuckPondIOManager(
                bucket_name="datalake",
                duckdb_options=DUCKDB_LOCALSTACK_OPTIONS,
            )
        }
    )
