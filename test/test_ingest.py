import pytest
from pyspark.sql import SparkSession
from pathlib import Path
import datetime
from test_de_python_v2.ingest_drug import ingest as ingest_drug
from test_de_python_v2.ingest_pubmed import ingest as ingest_pubmed
from test_de_python_v2.find_references import find_references


class Test_ingest:

    def test_ingest_drug(self, datadir):
        spark = SparkSession.builder.getOrCreate()
        ingest_drug(spark, datadir)
        data = spark.read.parquet(str(datadir / 'parquet-drug')).collect()
        assert len(data) == 7
        for row in data:
            if row.atccode == 'A03BA':
                assert row.drug == 'ATROPINE'

    def test_ingest_pubmed(self, datadir):
        spark = SparkSession.builder.getOrCreate()
        ingest_pubmed(spark, datadir)
        data = spark.read.parquet(str(datadir / 'parquet-pubmed')).collect()
        assert len(data) == 8
        for row in data:
            if row.id == 6:
                assert row.title == 'Rapid reacquisition of contextual fear following extinction in mice: effects of amount of extinction, tetracycline acute ethanol withdrawal, and ethanol intoxication.'
                assert row.date == datetime.date(day=1, month=1, year=2020)
            if row.id == 7:
                assert row.title == 'The High Cost of Epinephrine Autoinjectors and Possible Alternatives.'
                assert row.date == datetime.date(day=1, month=2, year=2020)

    def test_find_references(self, datadir):
        spark = SparkSession.builder.getOrCreate()
        ingest_drug(spark, datadir)
        ingest_pubmed(spark, datadir)
        find_references(spark, datadir)
