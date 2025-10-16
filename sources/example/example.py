import json
import random
from typing import Dict, List, Tuple, Any, Iterator
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# This is an example implementation of the LakeflowConnect class.
class LakeflowConnect():
    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize source parameters. Options may include authentication or other configs.
        """
        self.options = options
        self.tables = ["my_table", "your_table"]
        self.offset_id = 0
        self.offset_key = 1000

    def list_tables(self) -> List[str]:
        """
        Returns a list of available tables.
        """
        return self.tables

    def get_table_details(self, table: str) -> Tuple[StructType, Dict[str, str]]:
        """
        Returns the schema and metadata for a given table using StructType.
        """
        if table == "my_table":
            schema = StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("name", StringType(), True),
                ]
            )
            metadata = {"primary_key": "id", "ingestion_types": "append"}
        elif table == "your_table":
            schema = StructType(
                [
                    StructField("key", StringType(), False),
                    StructField("value", StringType(), True),
                ]
            )
            metadata = {"primary_key": "key", "ingestion_types": "append"}
        else:
            raise ValueError(f"Unknown table: {table}")

        return schema, metadata

    def read_table(self, table_name: str, start_offset: dict) -> (Iterator[dict], dict):
        """
        Read data from a table and return an iterator of records along with the next offset.
        """
        # Call the helper function to get the iterator
        data_iterator = self._read_helper(table_name, start_offset)

        # Calculate the next offset (increment by 10 since helper returns 10 records)
        current_offset = (
            int(start_offset)
            if start_offset
            else (self.offset_id if table_name == "my_table" else self.offset_key)
        )
        next_offset = current_offset + 10

        return data_iterator, {"offset": next_offset}

    def _read_helper(self, table_name: str, start_offset: dict) -> Iterator[dict]:
        if table_name not in self.tables:
            raise ValueError(f"Unknown table: {table_name}")

        current_offset = (
            int(start_offset)
            if start_offset
            else (self.offset_id if table_name == "my_table" else self.offset_key)
        )

        for i in range(10):
            record = {
                "id": current_offset
                if table_name == "my_table"
                else str(current_offset),
                "name"
                if table_name == "my_table"
                else "value": f"Value_{random.randint(1000, 9999)}",
            }
            current_offset += 1

            yield json.dumps(record)


