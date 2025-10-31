from pyspark.sql.types import *
from pyspark.sql import SparkSession, Row
from pyspark.sql.datasource import (
    DataSource,
    SimpleDataSourceStreamReader,
    InputPartition,
    DataSourceReader,
)
from typing import Iterator

METADATA_TABLE = "_lakeflow_metadata"
TABLE_NAME = "tableName"
TABLE_NAME_LIST = "tableNameList"

class LakeflowStreamReader(SimpleDataSourceStreamReader):
    def __init__(self, options, schema, lakeflow_connect: LakeflowConnect):
        self.options = options
        self.lakeflow_connect = lakeflow_connect
        self.schema = schema

    def initialOffset(self):
        return {}

    def read(self, start: dict) -> (Iterator[tuple], dict):
        records, offset = self.lakeflow_connect.read_table(
            self.options["tableName"], start
        )
        rows = map(lambda x: parse_value(x, self.schema), records)
        return rows, offset

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[tuple]:
        return self.read(start)[0]


class LakeflowBatchReader(DataSourceReader):
    def __init__(self, options, schema, lakeflow_connect):
        self.options = options
        self.schema = schema
        self.lakeflow_connect = lakeflow_connect
        self.table_name = options["tableName"]

    def read(self, partition):
        all_records = []
        if self.table_name == METADATA_TABLE:
            all_records = self._read_table_metadata()
        else:
            all_records, _ = self.lakeflow_connect.read_table(self.table_name, None)

        rows = map(lambda x: parse_value(x, self.schema), all_records)
        return iter(rows)

    def _read_table_metadata(self):
        table_name_list = self.options.get(TABLE_NAME_LIST, "")
        table_names = [o.strip() for o in table_name_list.split(",") if o.strip()]
        all_records = []
        for table in table_names:
            metadata = self.lakeflow_connect.read_table_metadata(table)
            all_records.append({
                "tableName": table,
                "primaryKey": metadata["primary_key"],
                "cursorField": metadata["cursor_field"],
                "ingestionType": metadata["ingestion_type"],
            })
        map(lambda x: parse_value(x, self.schema), all_records)


class LakeflowSource(DataSource):
    def __init__(self, options):
        self.options = options
        self.lakeflow_connect = LakeflowConnect(options)

    @classmethod
    def name(cls):
        return "lakeflow_connect"

    def schema(self):
        table = self.options["tableName"]
        if table == METADATA_TABLE:
            return StructType([
                StructField("tableName", StringType(), False),
                StructField("primaryKey", ArrayType(StringType()), True),
                StructField("cursorField", ArrayType(StringType()), True),
                StructField("ingestionType", StringType(), True),
            ])
        else:
            # Assuming the LakeflowConnect interface uses get_table_schema, not get_table_details
            return self.lakeflow_connect.get_table_schema(table)

    def reader(self, schema):
        return LakeflowBatchReader(self.options, schema, self.lakeflow_connect)

    def simpleStreamReader(self, schema):
        return LakeflowStreamReader(self.options, schema, self.lakeflow_connect)


spark.dataSource.register(LakeflowSource)
