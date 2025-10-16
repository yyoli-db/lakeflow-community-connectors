from pyspark.sql.types import *
from pyspark.sql import SparkSession, Row
from pyspark.sql.datasource import (
    DataSource,
    SimpleDataSourceStreamReader,
    InputPartition,
    DataSourceReader,
)
from typing import Iterator


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
        all_records, _ = self.lakeflow_connect.read_table(self.table_name, None)
        rows = map(lambda x: parse_value(x, self.schema), all_records)
        return iter(rows)


class LakeflowSource(DataSource):
    def __init__(self, options):
        self.options = options
        self.lakeflow_connect = LakeflowConnect(options)

    @classmethod
    def name(cls):
        return "lakeflow_connect"

    def schema(self):
        table = self.options["tableName"]
        return self.lakeflow_connect.get_table_details(table)[0]

    def reader(self, schema):
        return LakeflowBatchReader(self.options, schema, self.lakeflow_connect)

    def simpleStreamReader(self, schema):
        return LakeflowStreamReader(self.options, schema, self.lakeflow_connect)

