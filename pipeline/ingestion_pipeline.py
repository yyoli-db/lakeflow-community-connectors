import dlt
from pyspark.sql.functions import col, expr
from libs.spec import SpecParser
from pipeline_spec import *

spec = SpecParser(pipeline_spec)
connection_name = spec.connection_name()
table_list = spec.get_tables()

def _create_cdc_table(table, primary_key, cursor_field, view_name):
    """Create CDC table using streaming and apply_changes"""
    @dlt.view(name=view_name)
    def v():
        return (
            spark.readStream.format("lakeflow_connect")
                .option("databricks.connection", connection_name)
                .option("tableName", table)
                .load()
        )
    dlt.create_streaming_table(name=table)
    dlt.apply_changes(
        target=table,
        source=view_name,
        keys=[primary_key] if isinstance(primary_key, str) else primary_key,
        sequence_by=col(cursor_field),
        stored_as_scd_type="1",
    )


def _create_snapshot_table(table, primary_key, view_name):
    """Create snapshot table using batch read and apply_changes_from_snapshot"""
    @dlt.view(name=view_name)
    def snapshot_view():
        return (
            spark.read.format("lakeflow_connect")
                .option("databricks.connection", connection_name)
                .option("tableName", table)
                .load()
        )
    dlt.create_streaming_table(name=table)
    dlt.apply_changes_from_snapshot(
        target=table,
        source=view_name,
        keys=[primary_key] if isinstance(primary_key, str) else primary_key,
        stored_as_scd_type="1",
    )

def _create_append_table(table, view_name):
    """Create append table using streaming without apply_changes"""
    @dlt.table(name=table)
    def append_table():
        return (
            spark.readStream.format("lakeflow_connect")
                .option("databricks.connection", connection_name)
                .option("tableName", table)
                .load()
        )


def _get_table_metadata() -> dict:
    df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", connection_name)
        .option("tableName", "_lakeflow_metadata")
        .load()
    )
    metadata = {}
    for row in df.collect():
        metadata[row["tableName"]] = {
            "primaryKey": row["primaryKey"] or [],
            "cursorField": row["cursorField"] or [],
            "ingestionType": row["ingestionType"] or "cdc"
        }
    return metadata



# Function to ingest a list of objects
def ingest(allow_list: list[str]):
    metadata = _get_table_metadata()
    def _create_table(table):
        primary_key = metadata[table]["primary_key"]
        cursor_field = metadata[table]["cursor_field"]
        ingestion_type = metadata[table].get("ingestion_type", "cdc")
        view_name = table + "_staging"

        if ingestion_type == "cdc":
            _create_cdc_table(table, primary_key, cursor_field, view_name)
        elif ingestion_type == "snapshot":
            _create_snapshot_table(table, primary_key, view_name)
        elif ingestion_type == "append":
            _create_append_table(table, view_name)

    for table_name in allow_list:
        _create_table(table_name)


ingest(table_list)
