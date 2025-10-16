import dlt
from pyspark.sql.functions import col, expr

# Get authentication parameters
# This is using the secret service
# TODO: support a generic UC connection
secret_names = [secret.key for secret in dbutils.secrets.list(scope=connection_name)]
secret_options = {secret_name: str(dbutils.secrets.get(scope=connection_name, key=secret_name)) for secret_name in secret_names}
lakeflow_connect = LakeflowConnect(secret_options)


def _create_cdc_table(table, primary_key, cursor_field, view_name):
    """Create CDC table using streaming and apply_changes"""
    @dlt.view(name=view_name)
    def v():
        return (
            spark.readStream.format("lakeflow_connect")
                .options(**secret_options)
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
                .options(**secret_options)
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
                .options(**secret_options)
                .option("tableName", table)
                .load()
        )


# Function to ingest a list of objects
def ingest(allow_list: list[str]):
    def _create_table(table):
        metadata = lakeflow_connect.get_table_details(table)[1]
        primary_key = metadata["primary_key"]
        cursor_field = metadata["cursor_field"]
        ingestion_type = metadata.get("ingestion_type", "cdc")
        view_name = table + "_staging"

        if ingestion_type == "cdc":
            _create_cdc_table(table, primary_key, cursor_field, view_name)
        elif ingestion_type == "snapshot":
            _create_snapshot_table(table, primary_key, view_name)
        elif ingestion_type == "append":
            _create_append_table(table, view_name)

    for table_name in allow_list:
        _create_table(table_name)


# Register the sources
spark.dataSource.register(LakeflowSource)

# Ingest objects from the list
ingest(object_list)
