import requests
import base64
import json
from pyspark.sql.types import *
from datetime import datetime
from typing import Iterator, Any


# This is the class each source connector needs to implement.
# !! DO NOT CHANGE THE CLASS NAME OR CREATE AN SUBCLASS OF THIS CLASS !! Please directly implement the methods below.
class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the source connector with parameters needed to connect to the source.
        Args:
            options: A dictionary of parameters like authentication tokens, table names, and other configurations.
        """

    def list_tables(self) -> list[str]:
        """
        List names of all the tables supported by the source connector.
        The list could either be a static list or retrieved from the source via API.
        Returns:
            A list of table names.
        """

    def get_table_details(self, table_name: str) -> (StructType, dict):
        """
        Fetch the schema and metadata of a table.
        Args:
            table_name: The name of the table to fetch the schema and metadata for.
        Returns:
            A tuple of (schema, metadata).
            schema: A StructType object representing the schema of the table.
            metadata: A dictionary containing the metadata of the table. It should include the following keys:
                - primary_key: The name of the primary key of the table.
                - cursor_field: The name of the field to use as a cursor for incremental loading.
                - ingestion_type: The type of ingestion to use for the table. It should be one of the following values:
                    - "snapshot": For snapshot loading.
                    - "cdc": capture incremental changes
                    - "append": incremental append
        """

    def read_table(self, table_name: str, start_offset: dict) -> (Iterator[dict], dict):
        """
        Read the records of a table and return an iterator of records and an offset.
        The read starts from the provided start_offset.
        Records returned in the iterator will be one batch of records marked by the offset as its end_offset.
        The read_table function could be called multiple times to read the entire table in multiple batches and
        it stops when the same offset is returned again.
        If the table cannot be incrementally read, the offset can be None if we want to read the entire table in one batch.
        We could still return some fake offsets (cannot checkpointing) to split the table into multiple batches.
        Args:
            table_name: The name of the table to read.
            start_offset: The offset to start reading from.
        Returns:
            An iterator of records in JSON format and an offset.
            records: An iterator of records in JSON format.
            offset: An offset in dict.
        """

