import pytest
from pyspark.sql import SparkSession
from pathlib import Path
from test_de_python_v2.ingest_drugs import ingest


class Test_ingest:

    def test_ingest_drugs(self, datadir):
        spark = SparkSession.builder.getOrCreate()
        ingest(spark, datadir)
        data = spark.read.parquet(str(datadir / 'parquet')).collect()
        assert len(data) == 7
        for row in data:
            if row.atccode == 'A03BA':
                assert row.drug == 'ATROPINE'
